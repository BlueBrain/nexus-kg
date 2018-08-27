package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlClient}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{PersistenceConfig, SparqlConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.akka.iri._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.jena.query.ResultSet

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Indexer which takes a resource event and calls SPARQL client with relevant update if required
  *
  * @param client    the SPARQL client
  * @param resources the resources operations
  */
private class SparqlIndexer[F[_]](client: SparqlClient[F], resources: Resources[F])(
    implicit F: MonadError[F, Throwable],
    labeledProject: LabeledProject) {

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
      case None => F.raiseError(NotFound(ev.id.ref))
      case Some(resource) =>
        fetchRevision(ev.id) flatMap {
          case Some(rev) if resource.rev > rev => indexResource(resource)
          case None                            => indexResource(resource)
          case _                               => F.pure(())
        }
    }
  }

  private def query(id: ResId) =
    s"SELECT ?o WHERE {<${id.value.show}> <${nxv.rev.value.show}> ?o} LIMIT 1"

  private def fetchRevision(id: ResId): F[Option[Long]] =
    client.queryRs(query(id)).map { rs =>
      Try(rs.next().getLiteral("o").getLong).toOption
    }

  private def indexResource(res: Resource): F[Unit] =
    resources.materializeWithMeta(res).value.flatMap {
      case Left(err) => F.raiseError(err)
      case Right(r)  => client.replace(res.id, r.value.graph)
    }

  private implicit def toGraphUri(id: ResId): Uri = id.value + "graph"
}

object SparqlIndexer {

  /**
    * Starts the index process for an sparql client
    *
    * @param view           the view for which to start the index
    * @param resources      the resources operations
    * @param labeledProject project to which the resource belongs containing label information (account label and project label)
    */
  // $COVERAGE-OFF$
  final def start(view: SparqlView, resources: Resources[Task], labeledProject: LabeledProject)(
      implicit
      as: ActorSystem,
      s: Scheduler,
      ucl: HttpClient[Task, ResultSet],
      config: SparqlConfig,
      persistence: PersistenceConfig): ActorRef = {

    implicit val mt = ActorMaterializer()
    implicit val ul = HttpClient.taskHttpClient
    implicit val lb = labeledProject

    val properties: Map[String, String] = {
      val props = new Properties()
      props.load(getClass.getResourceAsStream("/blazegraph/index.properties"))
      props.asScala.toMap
    }

    val client  = BlazegraphClient[Task](config.base, view.name, config.akkaCredentials)
    val indexer = new SparqlIndexer(client, resources)
    SequentialTagIndexer.startLocal[Event](
      () => client.createNamespaceIfNotExist(properties).runAsync.map(_ => ()),
      (ev: Event) => indexer(ev).runAsync,
      persistence.queryJournalPlugin,
      tag = s"project=${view.ref.id}",
      name = s"sparql-indexer-${view.name}"
    )
  }
  // $COVERAGE-ON$
}
