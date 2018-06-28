package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlClient}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.SparqlConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.{InProjectResolution, Resolution}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.akka.iri._
import ch.epfl.bluebrain.nexus.service.indexer.persistence.SequentialTagIndexer
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.jena.query.ResultSet

import scala.util.Try

/**
  * Indexer which takes a resource event and calls SPARQL client with relevant update if required
  *
  * @param client the SPARQL client
  */
class SparqlIndexer[F[_]: Resolution](client: SparqlClient[F])(implicit repo: Repo[F],
                                                               F: MonadError[F, Throwable],
                                                               ucl: HttpClient[F, ResultSet]) {

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
    fetch(ev.id, None).value.flatMap {
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
    s"SELECT ?o WHERE {<${id.value.show}> <${nxv.rev.value.show}> ?o}"

  private def fetchRevision(id: ResId): F[Option[Long]] =
    client.queryRs(query(id)).map { rs =>
      Try(rs.next().getLiteral("o").getLong).toOption
    }

  private def indexResource(res: Resource): F[Unit] =
    materialize(res).value.flatMap {
      case Left(err) => F.raiseError(err)
      case Right(r)  => client.replace(res.id, r.value.graph)
    }

  private implicit def toGraphUri(id: ResId): Uri = id.value + "graph"
}

object SparqlIndexer {

  /**
    * Starts the index process for an sparql client
    *
    * @param view     the view for which to start the index
    * @param pluginId the persistence query plugin id to query the event log
    */
  final def start(
      view: View,
      pluginId: String
  )(implicit repo: Repo[Task],
    as: ActorSystem,
    s: Scheduler,
    ucl: HttpClient[Task, ResultSet],
    config: SparqlConfig): ActorRef = {

    implicit val mt  = ActorMaterializer()
    implicit val ul  = HttpClient.taskHttpClient
    implicit val res = InProjectResolution[Task](view.ref)

    val client  = BlazegraphClient[Task](config.base, view.name, config.akkaCredentials)
    val indexer = new SparqlIndexer(client)
    SequentialTagIndexer.startLocal[Event](
      (ev: Event) => indexer(ev).runAsync,
      pluginId,
      tag = s"project=${view.ref.id}",
      name = s"sparql-indexer-${view.name}"
    )
  }
}
