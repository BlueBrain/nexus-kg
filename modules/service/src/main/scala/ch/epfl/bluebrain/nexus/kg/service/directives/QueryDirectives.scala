package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive, Directive1, MalformedQueryParamRejection}
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.WrongOrInvalidJson
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, Sort, SortList}
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.contexts.JenaExpander._
import ch.epfl.bluebrain.nexus.kg.core.queries.JsonLdFormat
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Filter, FilteringSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.{IllegalFilterFormat, IllegalOutputFormat}
import io.circe.parser.{decode, _}
import io.circe.{DecodingFailure, Json, ParsingFailure}

import scala.concurrent.Future
import scala.util.Success

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
      case (from, size) => Pagination(from.max(0), size.max(1).min(qs.maxSize))
    }

  /**
    * Extracts the ''context'' query param from the request.
    */
  def context(implicit fs: FilteringSettings, ctxs: Contexts[Future]): Directive1[Json] = {

    def resolveContext(context: Json = Json.obj()): Directive1[Json] =
      onComplete(ctxs.resolve(context.deepMerge(fs.ctx))).flatMap {
        case Success(expanded) => provide(expanded)
        case _ =>
          reject(
            MalformedQueryParamRejection("context",
                                         "IllegalContextExpansion",
                                         Some(WrongOrInvalidJson(Some("The context resolution did not succeed")))))
      }

    parameter('context.?).flatMap {
      case Some(contextString) =>
        parse(contextString) match {
          case Left(_: ParsingFailure) =>
            reject(
              MalformedQueryParamRejection("context",
                                           "IllegalContextFormat",
                                           Some(WrongOrInvalidJson(Some("The context format is invalid")))))
          case Right(context) => resolveContext(context)
        }
      case None => resolveContext()
    }
  }

  /**
    * Extracts the ''filter'' query param from the request.
    *
    * @param ctx the context to apply for the filter
    */
  def filtered(ctx: Json)(implicit fs: FilteringSettings): Directive1[Option[Filter]] = {
    implicit val _ = Filter.filterDecoder(ctx)
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
    * Extracts the ''published'' query param from the request.
    */
  def published: Directive1[Option[Boolean]] =
    parameter('published.as[Boolean].?).flatMap(opt => provide(opt))

  /**
    * Extracts the ''format'' query param from the request.
    */
  def format: Directive1[JsonLdFormat] =
    parameter('format.as[String].?).flatMap {
      case None => provide(JsonLdFormat.Default)
      case Some(format) =>
        JsonLdFormat.fromString(format) match {
          case Some(fmt) => provide(fmt)
          case None =>
            reject(
              MalformedQueryParamRejection("format",
                                           "IllegalOutputFormat",
                                           Some(IllegalOutputFormat(s"Unsupported JSON-LD output formats: '$format'"))))
        }
    }

  /**
    * Extracts the ''sort'' query param from the request.
    */
  def sort(ctx: Json): Directive1[SortList] =
    parameter('sort.?).flatMap {
      case Some(v) =>
        provide(
          SortList(v.split(",").map(Sort(_)).map { case Sort(order, value) => Sort(order, expand(value, ctx)) }.toList))
      case None => provide(SortList.Empty)
    }

  /**
    * Extracts the ''fields'' query param from the request.
    */
  def fields(ctx: Json): Directive1[Set[String]] =
    parameter('fields.?).flatMap {
      case Some(field) =>
        provide(field.split(",").map(_.trim).filterNot(_.isEmpty).map(field => expand(field, ctx)).toSet)
      case None => provide(Set.empty[String])

    }

  /**
    * Extracts the query parameters defined for search requests or set them to preconfigured values
    * if present.
    *
    * @param qs the preconfigured query settings
    * @param fs the preconfigured filtering settings
    */
  def searchQueryParams(implicit qs: QuerySettings, fs: FilteringSettings, ctxs: Contexts[Future])
    : Directive[(Pagination, Option[Filter], Option[String], Option[Boolean], Set[String], SortList)] =
    context.flatMap { jsonCtx =>
      paginated & filtered(jsonCtx) & q & deprecated & fields(jsonCtx) & sort(jsonCtx)
    }
}

object QueryDirectives extends QueryDirectives
