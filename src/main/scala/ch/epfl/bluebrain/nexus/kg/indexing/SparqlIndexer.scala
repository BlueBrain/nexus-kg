package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import cats.Monad
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlWriteQuery}
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer._
import ch.epfl.bluebrain.nexus.rdf.syntax.akka._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.{IndexerConfig, SequentialTagIndexer}
import io.circe.Json
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.jena.query.ResultSet

import scala.collection.JavaConverters._

private class SparqlIndexerMapping[F[_]](resources: Resources[F])(implicit F: Monad[F], project: Project) {

  private val logger: Logger = Logger[this.type]

  /**
    * When an event is received, the current state is obtained and a [[SparqlWriteQuery]] is built.
    *
    * @param event event to be mapped to a Sparql insert query
    */
  final def apply(event: Event): F[Option[Identified[ProjectRef, SparqlWriteQuery]]] =
    resources.fetch(event.id, None).value.flatMap {
      case None           => F.pure(None)
      case Some(resource) => buildInsertQuery(resource)
    }

  private def buildInsertQuery(res: Resource): F[Option[Identified[ProjectRef, SparqlWriteQuery]]] =
    resources.materializeWithMeta(res).value.map {
      case Left(e) =>
        logger.error(s"Unable to materialize with meta, due to '$e'")
        None
      case Right(r) =>
        Some(res.id -> SparqlWriteQuery.replace(toGraphUri(res.id), r.value.graph))
    }

  private def toGraphUri(id: ResId): Uri = (id.value + "graph").toAkkaUri
}

object SparqlIndexer {

  /**
    * Starts the index process for an sparql client
    *
    * @param view          the view for which to start the index
    * @param resources     the resources operations
    * @param project       the project to which the resource belongs
    * @param restartOffset a flag to decide whether to restart from the beginning or to resume from the previous offset
    */
  // $COVERAGE-OFF$
  final def start(view: SparqlView, resources: Resources[Task], project: Project, restartOffset: Boolean)(
      implicit as: ActorSystem,
      mt: ActorMaterializer,
      ul: UntypedHttpClient[Task],
      s: Scheduler,
      uclRs: HttpClient[Task, ResultSet],
      config: AppConfig): ActorRef = {

    import ch.epfl.bluebrain.nexus.kg.instances.retriableMonadError
    implicit val lb       = project
    implicit val uclJson  = HttpClient.withUnmarshaller[Task, Json]
    implicit val indexing = config.indexing.sparql

    val properties: Map[String, String] = {
      val props = new Properties()
      props.load(getClass.getResourceAsStream("/blazegraph/index.properties"))
      props.asScala.toMap
    }

    val client = BlazegraphClient[Task](config.sparql.base, view.name, config.sparql.akkaCredentials)
    val mapper = new SparqlIndexerMapping(resources)
    val init =
      for {
        _ <- client.createNamespace(properties)
        _ <- if (view.rev > 1) client.copy(namespace = view.copy(rev = view.rev - 1).name).deleteNamespace
        else Task.pure(true)
      } yield ()

    SequentialTagIndexer.start(
      IndexerConfig
        .builder[Task]
        .name(s"sparql-indexer-${view.name}")
        .tag(s"project=${view.ref.id}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry[RetriableErr](indexing.retry.retryStrategy)
        .batch(indexing.batch, indexing.batchTimeout)
        .restart(restartOffset)
        .init(init)
        .mapping(mapper.apply)
        .index(inserts => client.bulk(inserts.removeDupIds: _*))
        .build)
  }
  // $COVERAGE-ON$
}
