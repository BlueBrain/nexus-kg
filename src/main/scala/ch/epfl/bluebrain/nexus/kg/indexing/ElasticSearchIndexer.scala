package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.scaladsl.Source
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient.BulkOp
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressFlow.ProgressFlowElem
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress.NoProgress
import ch.epfl.bluebrain.nexus.sourcing.projections._
import journal.Logger

import scala.concurrent.ExecutionContext

// $COVERAGE-OFF$
@SuppressWarnings(Array("MaxParameters"))
object ElasticSearchIndexer {

  private implicit val log: Logger = Logger[ElasticSearchIndexer.type]

  /**
    * Starts the index process for an ElasticSearch client
    *
    * @param view          the view for which to start the index
    * @param resources     the resources operations
    * @param project       the project to which the resource belongs
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    */
  final def start[F[_]: Timer](
      view: ElasticSearchView,
      resources: Resources[F],
      project: Project,
      restartOffset: Boolean
  )(
      implicit as: ActorSystem,
      actorInitializer: (Props, String) => ActorRef,
      projections: Projections[F, String],
      F: Effect[F],
      clients: Clients[F],
      config: AppConfig
  ): StreamSupervisor[F, ProjectionProgress] = {

    implicit val ec: ExecutionContext          = as.dispatcher
    implicit val p: Project                    = project
    implicit val indexing: IndexingConfig      = config.elasticSearch.indexing
    implicit val metadataOpts: MetadataOptions = MetadataOptions(linksAsIri = true, expandedLinks = true)
    val client: ElasticSearchClient[F]         = clients.elasticSearch.withRetryPolicy(config.elasticSearch.indexing.retry)

    def deleteOrIndex(res: ResourceV): Option[BulkOp] =
      if (res.deprecated && !view.filter.includeDeprecated) Some(delete(res))
      else view.toDocument(res).map(doc => BulkOp.Index(view.index, res.id.value.asString, doc))

    def delete(res: ResourceV): BulkOp =
      BulkOp.Delete(view.index, res.id.value.asString)

    val initFetchProgressF: F[ProjectionProgress] =
      if (restartOffset)
        projections.recordProgress(view.progressId, NoProgress) >> view.createIndex >> F.pure(NoProgress)
      else view.createIndex >> projections.progress(view.progressId)

    val sourceF: F[Source[ProjectionProgress, _]] = initFetchProgressF.map { initial =>
      val flow = ProgressFlowElem[F, Any]
        .collectCast[Event]
        .groupedWithin(indexing.batch, indexing.batchTimeout)
        .distinct()
        .mapAsync(view.toResource(resources, _))
        .collectSome[ResourceV]
        .collect {
          case res if view.allowedSchemas(res) && view.allowedTypes(res) => deleteOrIndex(res)
          case res if view.allowedSchemas(res)                           => Some(delete(res))
        }
        .collectSome[BulkOp]
        .runAsyncBatch(client.bulk(_))()
        .mergeEmit()
        .toPersistedProgress(view.progressId, initial)
      cassandraSource(s"project=${view.ref.id}", view.progressId, initial.minProgress.offset).via(flow)
    }
    StreamSupervisor.start(sourceF, view.progressId, actorInitializer)
  }
}
// $COVERAGE-ON$
