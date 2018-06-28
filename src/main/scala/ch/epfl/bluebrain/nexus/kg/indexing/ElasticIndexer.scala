package ch.epfl.bluebrain.nexus.kg.indexing

import java.net.URLEncoder

import akka.http.scaladsl.model.Uri
import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.types.identity.Identity
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.Resolution
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.Resources._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.akka.iri._
import ch.epfl.bluebrain.nexus.service.http.Path
import io.circe.Json
import org.apache.jena.query.ResultSet
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexer._

import scala.util.Try

/**
  * Indexer which takes a resource event and calls ElasticSearch client with relevant update if required
  *
  * @param client the ElasticSearch client
  */
class ElasticIndexer[F[_]: Resolution](client: ElasticClient[F], project: ProjectRef)(implicit repo: Repo[F],
                                                                 config: ElasticConfig,
                                                                 F: MonadError[F, Throwable],
                                                                 ucl: HttpClient[F, ResultSet]) {
  private val revKey = "_rev"

  /**
    * When an event is received, the current state is obtained.
    * Afterwards, the current revision is fetched from the ElasticSearch index.
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

  private def fetchRevision(id: ResId): F[Option[Long]] =
    client.get[Json](elasticIndex(project), config.docType, id.elasticId, include = Set(revKey))
      .map(j => j.hcursor.get[Long](revKey).toOption)


  private def indexResource(res: Resource): F[Unit] =
    materialize(res).value.flatMap {
      case Left(err) => F.raiseError(err)
      case Right(r)  => client.update(elasticIndex(project), config.docType, r.id.elasticId, Json.obj())
    }

  private implicit def toGraphUri(id: ResId): Uri = id.value + "graph"
}

object ElasticIndexer {

  private[indexing] def elasticIndex(project: ProjectRef)(implicit config: ElasticConfig): String =
    URLEncoder.encode(s"${config.indexPrefix}_${project.id}", "UTF-8").toLowerCase

  private[indexing] implicit class ResIdSyntax(id: ResId) {

    def elasticId: String =
      URLEncoder.encode(id.value.show, "UTF-8").toLowerCase
  }
  object ElasticIds {

    /**
      * Generates the ElasticSearch index id from the provided ''identity''.
      *
      * @param identity the identity from where to generate the index.
      *                 Ids will look as follows: {prefix}_{identity_id_url_encoded}
      * @param config   the configuration from where to take the prefix for the id
      */
    private[indexing] implicit def indexId(identity: Identity)(implicit config: ElasticConfig): String =
      URLEncoder.encode(s"${config.indexPrefix}_${project}", "UTF-8").toLowerCase


  }
}
