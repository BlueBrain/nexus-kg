package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PaginationConfig

object QueryDirectives {

  /**
    * @return the extracted pagination form the request query parameters or defaults to the preconfigured values.
    */
  def paginated(implicit config: PaginationConfig): Directive1[Pagination] =
    (parameter('from.as[Int] ? config.pagination.from) & parameter('size.as[Int] ? config.pagination.size)).tmap {
      case (from, size) => Pagination(from.max(0), size.max(1).min(config.sizeLimit))
    }
}
