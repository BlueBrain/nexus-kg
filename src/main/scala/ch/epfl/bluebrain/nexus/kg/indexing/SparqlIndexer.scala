package ch.epfl.bluebrain.nexus.kg.indexing

import akka.http.scaladsl.model.Uri
import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.resolve.Resolution
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.{Url, Urn}
import ch.epfl.bluebrain.nexus.rdf.akka.iri._
import org.apache.jena.query.ResultSet

import scala.util.Try

/**
  * Indexer which takes a resource event and calls SPARQL client with relevant update if required
  *
  * @param client SPARQL client
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
    get(ev.id).value.flatMap {
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
    s"""
       |SELECT ?o WHERE {<${id.value.show}> <https://bbp.epfl.ch/nexus/v0/vocabs/nexus/core/terms/v0.1.0/rev> ?o}
    """.stripMargin

  private def fetchRevision(id: ResId): F[Option[Long]] =
    client.queryRs(query(id)).map { rs =>
      Try(rs.next().getLiteral("o").getLong).toOption
    }

  private def indexResource(res: Resource): F[Unit] =
    materialize(res).value.flatMap {
      case Left(err) => F.raiseError(err)
      case Right(r)  => client.replace(res.id, r.value.graph)
    }

  private implicit def toGraphUri(id: ResId): Uri =
    id.value match {
      case url: Url if url.path.endsWithSlash => url.copy(path = url.path + "graph")
      case urn: Urn if urn.nss.endsWithSlash  => urn.copy(nss = urn.nss + "graph")
      case url: Url                           => url.copy(path = url.path / "graph")
      case urn: Urn                           => urn.copy(nss = urn.nss / "graph")
    }
}

object SparqlIndexer {

  /**
    * @param client SPARQL client
    * @return anew [[SparqlIndexer]]
    */
  final def apply[F[_]: Resolution](client: SparqlClient[F])(implicit repo: Repo[F],
                                                             F: MonadError[F, Throwable],
                                                             ucl: HttpClient[F, ResultSet]): SparqlIndexer[F] =
    new SparqlIndexer(client)
}
