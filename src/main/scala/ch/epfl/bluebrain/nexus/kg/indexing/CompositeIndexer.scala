package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.regex.Pattern.quote

import akka.actor.{ActorRef, ActorSystem, Props}
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlWriteQuery
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlWriteQuery.replace
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.CompositeIndexer.ctx
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Projection
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Projection.{ElasticSearchProjection, SparqlProjection}
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.decoder.GraphDecoder.DecoderResult
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import ch.epfl.bluebrain.nexus.sourcing.projections._
import io.circe.Json
import journal.Logger
import kamon.Kamon
import monix.execution.atomic.AtomicLong

private class CompositeIndexerProjections[F[_]](view: CompositeView)(
    implicit F: Effect[F],
    config: AppConfig,
    project: Project,
    clients: Clients[F]
) {

  private val logger                 = Logger[this.type]
  private val metaKeys               = metaPredicates.map(_.prefix).toSeq
  private val metadataOptions        = MetadataOptions(linksAsIri = false, expandedLinks = true)
  private val FSome: F[Option[Unit]] = F.pure(Option(()))

  /**
    * After a resource gets indexed in the temporary namespace, a projection queries it, transforms it into a graph
    * and indexes into the corresponding projection.
    *
    * @param res resource to be mapped to a Elastic Search insert query
    */
  final def apply(res: ResourceV, command: SparqlWriteQuery): F[List[Unit]] = {
    val commands: Set[F[Option[Unit]]] = command match {
      case _: SparqlWriteQuery.SparqlDropQuery =>
        view.projections.map(deleteIndexId(res))
      case _ =>
        view.projections.filter(candidate(res, _)).map { projection =>
          query(projection, res).map(_.asGraph -> projection).flatMap[Option[Unit]] {
            case (None, _)                                 => deleteIndexId(res)(projection)
            case (Some(graph), _) if graph.triples.isEmpty => deleteIndexId(res)(projection)
            case (Some(graph), p: ElasticSearchProjection) =>
              toEsDocument(res, p, graph) match {
                case Some(doc) => clients.elasticSearch.create(p.view.index, res.id.value.asString, doc) >> FSome
                case None      => F.pure(None)
              }
            case (Some(graph), p: SparqlProjection) =>
              clients.sparql.copy(namespace = p.view.index).bulk(replace(res.id.toGraphUri, graph)) >> FSome
          }
        }
    }
    commands.toList.sequence.map(_.flatten)
  }

  private def deleteIndexId(res: ResourceV): Projection => F[Option[Unit]] = {
    case p: ElasticSearchProjection => clients.elasticSearch.delete(p.view.index, res.id.value.asString) >> FSome
    case p: SparqlProjection        => clients.sparql.copy(namespace = p.view.index).drop(res.id.toGraphUri) >> FSome
  }

  private def candidate(res: ResourceV, p: Projection): Boolean =
    validSchema(p.view, res) && validTypes(p.view, res) && view.filter.resourceTag.forall(res.tags.contains)

  private def toEsDocument(res: ResourceV, p: ElasticSearchProjection, graph: Graph): Option[Json] = {
    val rootNode      = IriNode(res.id.value)
    val projectionCtx = Json.obj("@context" -> p.context)
    val context       = if (p.view.includeMetadata) ctx.appendContextOf(projectionCtx) else projectionCtx

    def asJson(g: Graph): DecoderResult[Json] = RootedGraph(rootNode, g).as[Json](context)

    val transformed: DecoderResult[Json] = {

      val metaGraph    = if (p.view.includeMetadata) Graph(graph.triples ++ res.metadata(metadataOptions)) else graph
      val keysToRemove = if (p.view.includeMetadata) Seq.empty[String] else metaKeys

      if (p.view.sourceAsText)
        asJson(metaGraph.add(res.id.value, nxv.original_source, res.value.source.removeKeys(metaKeys: _*).noSpaces))
      else
        asJson(metaGraph).map(metaJson => res.value.source.removeKeys(keysToRemove: _*) deepMerge metaJson)
    }

    transformed match {
      case Left(err) =>
        logger.error(
          s"Could not convert resource with id '${res.id}' and value '${res.value}' from Graph back to json. Reason: '${err.message}'"
        )
        None
      case Right(value) => Some(value.removeKeys("@context"))
    }
  }

  private def query(p: Projection, res: ResourceV) =
    clients.sparql
      .copy(namespace = view.defaultSparqlView.index)
      .queryRaw(p.query.replaceAll(quote("{resource_id}"), s"<${res.id.value.asString}>"))
}

object CompositeIndexer {

  // $COVERAGE-OFF$
  /**
    * Starts the index process for an CompositeIndexer
    *
    * @param view          the view for which to start the index
    * @param resources     the resources operations
    * @param project       the project to which the resource belongs
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    */
  final def start[F[_]: Timer](
      view: CompositeView,
      resources: Resources[F],
      project: Project,
      restartOffset: Boolean
  )(
      implicit clients: Clients[F],
      as: ActorSystem,
      actorInitializer: (Props, String) => ActorRef,
      config: AppConfig,
      P: Projections[F, Event],
      F: Effect[F]
  ): StreamSupervisor[F, ProjectionProgress] = {

    val sparqlErrorMonadError             = ch.epfl.bluebrain.nexus.kg.instances.sparqlErrorMonadError
    val sparqlClient                      = clients.sparql.copy(namespace = view.defaultSparqlView.index)
    implicit val p: Project               = project
    implicit val indexing: IndexingConfig = config.sparql.indexing

    val mapper            = new SparqlIndexerMapping(view.defaultSparqlView, resources)
    val projectionIndexer = new CompositeIndexerProjections[F](view)

    val init = view.defaultSparqlView.createIndex >> view.projections.map(_.view.createIndex).toList.sequence >> F.unit

    val processedEventsGauge = Kamon
      .gauge("kg_indexer_gauge")
      .withTag("type", "composite")
      .withTag("project", project.show)
      .withTag("organization", project.organizationLabel)
      .withTag("viewId", view.id.show)
    val processedEventsCounter = Kamon
      .counter("kg_indexer_counter")
      .withTag("type", "composite")
      .withTag("project", project.show)
      .withTag("organization", project.organizationLabel)
      .withTag("viewId", view.id.show)
    val processedEventsCount = AtomicLong(0L)

    // TODO: Retry SparqlServerOrUnexpectedFailure and ElasticSearchServerOrUnexpectedFailure
    TagProjection.start(
      ProjectionConfig
        .builder[F]
        .name(s"composite-indexer-${view.name}")
        .tag(s"project=${view.ref.id}")
        .actorOf(actorInitializer)
        .plugin(config.persistence.queryJournalPlugin)
        .retry[SparqlServerOrUnexpectedFailure](indexing.retry.retryStrategy)(sparqlErrorMonadError)
        .batch(indexing.batch, indexing.batchTimeout)
        .restart(restartOffset)
        .init(init)
        .mapping(mapper.apply)
        .index { inserts =>
          val uniqueInserts = inserts.groupBy { case (res, _) => res.id }.values.flatMap(_.lastOption).toList
          sparqlClient.bulk(uniqueInserts.map { case (_, elem) => elem }: _*) >>
            uniqueInserts.map { case (res, cmd) => projectionIndexer(res, cmd) }.sequence.map(_.flatten) >> F.unit
        }
        .mapInitialProgress { p =>
          processedEventsCount.set(p.processedCount)
          processedEventsGauge.update(p.processedCount.toDouble)
          F.unit
        }
        .mapProgress { p =>
          val previousCount = processedEventsCount.get()
          processedEventsGauge.update(p.processedCount.toDouble)
          processedEventsCounter.increment(p.processedCount - previousCount)
          processedEventsCount.set(p.processedCount)
          F.unit
        }
        .build
    )
  }
  // $COVERAGE-ON$

  private[indexing] val ctx: Json = jsonContentOf("/elasticsearch/default-context.json")

}
