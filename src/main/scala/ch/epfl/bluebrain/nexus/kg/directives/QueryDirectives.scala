package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PaginationConfig
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

object QueryDirectives {

  /**
    * @return the extracted pagination from the request query parameters or defaults to the preconfigured values.
    */
  def paginated(implicit config: PaginationConfig): Directive1[Pagination] =
    (parameter('from.as[Int] ? config.pagination.from) & parameter('size.as[Int] ? config.pagination.size)).tmap {
      case (from, size) => Pagination(from.max(0), size.max(1).min(config.sizeLimit))
    }

  /**
    * @return the extracted search parameters from the request query parameters.
    */
  def searchParams(implicit project: Project): Directive1[SearchParams] =
    (parameter('deprecated.as[Boolean].?) &
      parameter('rev.as[Long].?) &
      parameter('schema.as[AbsoluteIri].?) &
      parameter('createdBy.as[AbsoluteIri].?) &
      parameter('updatedBy.as[AbsoluteIri].?) &
      parameter('type.as[VocabAbsoluteIri].*) &
      parameter('id.as[AbsoluteIri].?)).tmap {
      case (deprecated, rev, schema, createdBy, updatedBy, tpe, id) =>
        SearchParams(deprecated, rev, schema, createdBy, updatedBy, tpe.map(_.value).toList, id)
    }
}
