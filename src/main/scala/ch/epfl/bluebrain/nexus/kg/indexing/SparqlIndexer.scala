package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import cats.MonadError
import cats.syntax.flatMap._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlClient}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.serializers.Serializer._
import ch.epfl.bluebrain.nexus.rdf.syntax.akka._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.{IndexerConfig, SequentialTagIndexer}
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.jena.query.ResultSet

import scala.collection.JavaConverters._

/**
  * Indexer which takes a resource event and calls SPARQL client with relevant update if required
  *
  * @param client    the SPARQL client
  * @param resources the resources operations
  */
private class SparqlIndexer[F[_]](client: SparqlClient[F], resources: Resources[F])(
    implicit F: MonadError[F, Throwable],
    project: Project) {

  /**
    * When an event is received, the current state is obtained.
    * Afterwards, the current revision is fetched from the SPARQL index.
    * If the current revision is not found or it is smaller than the state's revision, the state gets indexed.
    * Otherwise the event it is skipped.
    *
    * @param ev event to index
    * @return Unit wrapped in the context F.
    *         This method will raise errors if something goes wrong
    */
  final def apply(ev: Event): F[Unit] = {
    resources.fetch(ev.id, None).value.flatMap {
      case None           => F.raiseError(NotFound(ev.id.ref))
      case Some(resource) => indexResource(resource)
    }
  }

  private def indexResource(res: Resource): F[Unit] =
    resources.materializeWithMeta(res).value.flatMap {
      case Left(err) => F.raiseError(err)
      case Right(r)  => client.replace(res.id, r.value.graph)
    }

  private implicit def toGraphUri(id: ResId): Uri = (id.value + "graph").toAkkaUri
}

object SparqlIndexer {

  /**
    * Starts the index process for an sparql client
    *
    * @param view      the view for which to start the index
    * @param resources the resources operations
    * @param project   the project to which the resource belongs
    */
  // $COVERAGE-OFF$
  final def start(view: SparqlView, resources: Resources[Task], project: Project)(implicit as: ActorSystem,
                                                                                  mt: ActorMaterializer,
                                                                                  ul: UntypedHttpClient[Task],
                                                                                  s: Scheduler,
                                                                                  uclRs: HttpClient[Task, ResultSet],
                                                                                  config: AppConfig): ActorRef = {

    implicit val lb      = project
    implicit val uclJson = HttpClient.withUnmarshaller[Task, Json]

    val properties: Map[String, String] = {
      val props = new Properties()
      props.load(getClass.getResourceAsStream("/blazegraph/index.properties"))
      props.asScala.toMap
    }

    val client  = BlazegraphClient[Task](config.sparql.base, view.name, config.sparql.akkaCredentials)
    val indexer = new SparqlIndexer(client, resources)
    val init = () =>
      (for {
        _ <- client.createNamespace(properties)
        _ <- if (view.rev > 1) client.copy(namespace = view.copy(rev = view.rev - 1).name).deleteNamespace
        else Task.pure(true)
      } yield ()).runToFuture

    SequentialTagIndexer.start(
      IndexerConfig.builder
        .name(s"sparql-indexer-${view.name}")
        .tag(s"project=${view.ref.id}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry(config.indexing.retry.maxCount, config.indexing.retry.strategy)
        .batch(config.indexing.batch, config.indexing.batchTimeout)
        .init(init)
        .index((l: List[Event]) => Task.sequence(l.removeDupIds.map(indexer(_))).map(_ => ()).runToFuture)
        .build)
  }
  // $COVERAGE-ON$
}
