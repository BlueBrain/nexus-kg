package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.instances.future._
import ch.epfl.bluebrain.nexus.kg.core.instances.{InstanceId, InstanceRef}
import ch.epfl.bluebrain.nexus.kg.indexing.query.{ QuerySettings, SparqlQuery}
import ch.epfl.bluebrain.nexus.kg.service.routes.SearchRoutes.InstanceIdCustomEncoders
import io.circe.generic.auto._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder
import ch.epfl.bluebrain.nexus.kg.service.query.FilterQueries
import io.circe.{Encoder, Json}
import kamon.akka.http.KamonTraceDirectives._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Http route definitions for search specific functionality.
  *
  * @param base          the service public uri + prefix
  * @param queryBuilder  query builder for search
  * @param querySettings the configurable settings specific to queries.
  */
class SearchRoutes(base: Uri, queryBuilder: FilterQueries[InstanceId], querySettings: QuerySettings) {

  private val exceptionHandler = ExceptionHandling.exceptionHandler
  private val customEncoders = new InstanceIdCustomEncoders(base)

  import customEncoders._, queryBuilder._

  def routes: Route = handleExceptions(exceptionHandler) {
    (pathPrefix("data") & pathEndOrSingleSlash) {
      get {
        val page = querySettings.pagination
        parameters(('q.as[String], 'from.as[Long] ? page.from, 'size.as[Int] ? page.size)) {
          (q, from, size) =>
            traceName("fullTextSearch") {
              queryBuilder.fullTextSearchQuery(q, Pagination(from, size)).response
            }
        }
      }
    }
  }
}

object SearchRoutes {

  /**
    * Constructs a ''SearchRoutes'' instance that defines http routes specific to search.
    *
    * @param client        the sparql client
    * @param base          the service public uri + prefix
    * @param querySettings the configurable settings specific to queries.
    */
  final def apply(client: SparqlClient[Future], base: Uri, querySettings: QuerySettings)
    (implicit ec: ExecutionContext): SearchRoutes = {
    new SearchRoutes(base, new FilterQueries(SparqlQuery[Future](client), querySettings), querySettings)
  }

  private class InstanceIdCustomEncoders(base: Uri)(implicit le: Encoder[Link]) extends RoutesEncoder[InstanceId, InstanceRef](base) {

    implicit val qualifierSchema: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)

    implicit val instanceIdWithLinksEncoder: Encoder[InstanceId] = Encoder.encodeJson.contramap { instanceId =>
      idWithLinksEncoder.apply(instanceId) deepMerge
        Json.obj("links" -> Json.arr(
          le(Link(rel = "self", href = instanceId.qualifyAsString)),
          le(Link(rel = "schema", href = instanceId.schemaId.qualifyAsString))
        ))
    }
  }

}

