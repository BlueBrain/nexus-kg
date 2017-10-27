package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive, Directive1, MalformedQueryParamRejection}
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.WrongOrInvalidJson
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Filter, FilteringSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalFilterFormat
import io.circe.parser.decode
import io.circe.{DecodingFailure, ParsingFailure}

/**
  * Collection of query specific directives.
  */
trait QueryDirectives {

  /**
    * Extracts pagination specific query params from the request or defaults to the preconfigured values.
    *
    * @param qs the preconfigured query settings
    */
  def paginated(implicit qs: QuerySettings): Directive1[Pagination] =
    (parameter('from.as[Int] ? qs.pagination.from) & parameter('size.as[Int] ? qs.pagination.size)).tmap {
      case (from, size) => Pagination(from, size)
    }

  /**
    * Extracts the ''filter'' query param from the request.
    *
    * @param fs the preconfigured filtering settings
    */
  def filtered(implicit fs: FilteringSettings): Directive1[Option[Filter]] =
    parameter('filter.?).flatMap {
      case Some(filterString) =>
        decode[Filter](filterString) match {
          case Left(_: ParsingFailure) =>
            reject(
              MalformedQueryParamRejection("filter",
                                           "IllegalFilterFormat",
                                           Some(WrongOrInvalidJson(Some("The filter format is invalid")))))
          case Left(df: DecodingFailure) =>
            reject(
              MalformedQueryParamRejection("filter",
                                           "IllegalFilterFormat",
                                           Some(IllegalFilterFormat(df.message, df.history.reverse.mkString("/")))))
          case Right(filter) =>
            provide(Some(filter))
        }
      case None =>
        provide(None)
    }

  /**
    * Extracts the ''q'' query param from the request. This param will be used as a full text search
    */
  def q: Directive1[Option[String]] =
    parameter('q.?).flatMap(opt => provide(opt))

  /**
    * Extracts the ''deprecated'' query param from the request.
    */
  def deprecated: Directive1[Option[Boolean]] =
    parameter('deprecated.as[Boolean].?).flatMap(opt => provide(opt))

  /**
    * Extracts the query parameters defined for search requests or set them to preconfigured values
    * if present.
    *
    * @param qs the preconfigured query settings
    * @param fs the preconfigured filtering settings
    */
  def searchQueryParams(
      implicit qs: QuerySettings,
      fs: FilteringSettings): Directive[(Pagination, Option[Filter], Option[String], Option[Boolean])] =
    paginated & filtered & q & deprecated

}

object QueryDirectives extends QueryDirectives
