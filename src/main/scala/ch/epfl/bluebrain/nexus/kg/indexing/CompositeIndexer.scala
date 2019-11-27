package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.SourceShape
import akka.stream.scaladsl._
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlWriteQuery}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{CompositeView, SingleView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressFlow.{Eval, PairMsg, ProgressFlowElem, ProgressFlowList}
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress.{NoProgress, OffsetsProgress}
import ch.epfl.bluebrain.nexus.sourcing.projections._
import journal.Logger

import scala.concurrent.ExecutionContext

// $COVERAGE-OFF$
@SuppressWarnings(Array("MaxParameters"))
object CompositeIndexer {

  private implicit val log: Logger = Logger[CompositeIndexer.type]

  /**
    * Starts the index process for a CompositeIndexer with the ability to reset the offset to NoOffset on the passed projections
    *
    * @param view               the view for which to start the index
    * @param resources          the resources operations
    * @param project            the project to which the resource belongs
    * @param restartOffsetViews the set of projection views for which the offset is restarted
    */
  final def start[F[_]: Timer: Clients: Effect](
      view: CompositeView,
      resources: Resources[F],
      project: Project,
      restartOffsetViews: Set[SingleView]
  )(
      implicit as: ActorSystem,
      actorInitializer: (Props, String) => ActorRef,
      config: AppConfig,
      projections: Projections[F, String]
  ): StreamSupervisor[F, ProjectionProgress] = {

    val progressId: String = view.defaultSparqlView.progressId

    val initialProgressF = projections
      .progress(progressId)
      .map {
        case progress: OffsetsProgress =>
          restartOffsetViews.foldLeft(progress) { (newProgress, pView) =>
            newProgress.replace(pView.progressId -> NoProgress)
          }
        case other => other // Do nothing, a composite view cannot be a single progress
      }
      .flatMap(newProgress => projections.recordProgress(progressId, newProgress) >> newProgress.pure[F])

    start(view, resources, project, initialProgressF)
  }

  /**
    * Starts the index process for a CompositeIndexer with the ability to restart the offset on the whole view
    *
    * @param view          the view for which to start the index
    * @param resources     the resources operations
    * @param project       the project to which the resource belongs
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    */
  final def start[F[_]: Timer: Clients](
      view: CompositeView,
      resources: Resources[F],
      project: Project,
      restartOffset: Boolean
  )(
      implicit as: ActorSystem,
      actorInitializer: (Props, String) => ActorRef,
      config: AppConfig,
      projections: Projections[F, String],
      F: Effect[F]
  ): StreamSupervisor[F, ProjectionProgress] = {

    val progressId: String = view.defaultSparqlView.progressId

    val initialProgressF: F[ProjectionProgress] =
      if (restartOffset) projections.recordProgress(progressId, NoProgress) >> F.pure(NoProgress)
      else projections.progress(progressId)

    start(view, resources, project, initialProgressF)
  }

  private def start[F[_]: Timer](
      view: CompositeView,
      resources: Resources[F],
      project: Project,
      initialProgressF: F[ProjectionProgress]
  )(
      implicit clients: Clients[F],
      as: ActorSystem,
      actorInitializer: (Props, String) => ActorRef,
      config: AppConfig,
      projections: Projections[F, String],
      F: Effect[F]
  ): StreamSupervisor[F, ProjectionProgress] = {
    val defaultView: SparqlView                         = view.defaultSparqlView
    val progressId: String                              = view.progressId
    val FSome: F[Option[Unit]]                          = F.pure(Option(()))
    implicit val ec: ExecutionContext                   = as.dispatcher
    implicit val proj: Project                          = project
    implicit val indexing: IndexingConfig               = config.sparql.indexing
    implicit val metadataOpts: MetadataOptions          = MetadataOptions(linksAsIri = true, expandedLinks = true)
    implicit val sparqlClientQuery: BlazegraphClient[F] = clients.sparql.copy(namespace = defaultView.index)
    val sparqlClientIndex: BlazegraphClient[F] =
      clients.sparql.copy(namespace = defaultView.index).withRetryPolicy(config.sparql.indexing.retry)

    def buildInsertOrDeleteQuery(res: ResourceV, view: SparqlView): SparqlWriteQuery =
      if (res.deprecated && !view.filter.includeDeprecated) view.buildDeleteQuery(res)
      else view.buildInsertQuery(res)

    val init = defaultView.createIndex >> view.projections.map(_.view.createIndex).toList.sequence >> F.unit

    val sourceF: F[Source[ProjectionProgress, _]] = (init >> initialProgressF).map { initial =>
      Source.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._
        // format: off
        val source: Source[PairMsg[Any], _] = cassandraSource(s"project=${view.ref.id}", progressId, initial.minProgress.offset)
        val mainFlow = ProgressFlowElem[F, Any]
          .collectCast[Event]
          .groupedWithin(indexing.batch, indexing.batchTimeout)
          .distinct()
          .mapAsync(view.toResource(resources, _))
          .collectSome[ResourceV]
          .collect {
            case res if defaultView.allowedSchemas(res) && defaultView.allowedTypes(res) => res -> buildInsertOrDeleteQuery(res, defaultView)
            case res if defaultView.allowedSchemas(res) =>                                  res -> defaultView.buildDeleteQuery(res)
          }
          .runAsyncBatch(list => sparqlClientIndex.bulk(list.map { case (_, bulkQuery) => bulkQuery }))(Eval.After(initial.progress(progressId).offset))
          .map { case (res, _) => res }
          .mergeEmit()

        val projectionsFlow = view.projections.map { projection =>
          val projView = projection.view
          ProgressFlowElem[F, ResourceV]
            .select(projView.progressId)
            .evaluateAfter(initial.progress(projView.progressId).offset)
            .collect {
              case res if projView.allowedSchemas(res) && projView.allowedTypes(res) && projView.allowedTag(res) => res
            }
            .mapAsync(res => projection.runQuery(res).map(res -> _.asGraph))
            .mapAsync {
              case (res, None)                                 => projView.deleteResource[F](res.id) >> FSome
              case (res, Some(graph)) if graph.triples.isEmpty => projView.deleteResource[F](res.id) >> FSome
              case (res, Some(graph))                          => projection.indexResourceGraph[F](res, graph)
            }
            .collectSome[Unit]
        }

        val broadcast = b.add(Broadcast[PairMsg[ResourceV]](view.projections.size))
        val merge     = b.add(ZipWithN[PairMsg[Unit], List[PairMsg[Unit]]](_.toList)(view.projections.size))

        val persistFlow = b.add(ProgressFlowList[F, Unit].mergeCombine().toPersistedProgress(progressId, initial))

        source ~> mainFlow.flow ~> broadcast
                                   projectionsFlow.foreach(broadcast ~> _.flow ~> merge)
                                                                                  merge ~> persistFlow.in

        SourceShape(persistFlow.out)
        // format: on
      })
    }
    StreamSupervisor.start(sourceF, progressId, actorInitializer)
  }
}
// $COVERAGE-ON$
