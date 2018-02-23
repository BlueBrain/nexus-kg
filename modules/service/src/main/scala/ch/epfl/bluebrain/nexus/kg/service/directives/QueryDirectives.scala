package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive, Directive1, MalformedQueryParamRejection, ValidationRejection}
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.WrongOrInvalidJson
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, SortList}
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.queries.Query.QueryPayload
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Filter, FilteringSettings}
import ch.epfl.bluebrain.nexus.kg.core.queries.{Field, JsonLdFormat}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.directives.StringUnmarshaller._
import ch.epfl.bluebrain.nexus.kg.service.query.QueryPayloadDecoder
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalPayload
import io.circe.parser._
import io.circe.{Decoder, Json}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Collection of query specific directives.
  */
trait QueryDirectives {

  def queryEntity(implicit fs: FilteringSettings, ctxs: Contexts[Future]): Directive1[QueryPayload] = {
    import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
    entity(as[Json]).flatMap { json =>
      onComplete(ctxs.resolve(json)) flatMap {
        case Success(jsonCtx) =>
          val queryDec = QueryPayloadDecoder(jsonCtx deepMerge fs.ctx)
          import queryDec._
          json.as[QueryPayload] match {
            case Right(query) => provide(query)
            case Left(err) =>
              reject(
                ValidationRejection("Error converting json to query",
                                    Some(IllegalPayload("Error converting json to query", Some(err.message)))))
          }
        case Failure(CommandRejected(contextRejection)) =>
          reject(
            ValidationRejection(
              "The context resolution did not succeed",
              Some(IllegalPayload(s"The context resolution did not succeed: $contextRejection", None))))
        case Failure(_) =>
          reject(
            ValidationRejection("The context resolution did not succeed",
                                Some(IllegalPayload("The context resolution did not succeed", None))))
      }
    }
  }

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

    def resolveContext(context: Json): Directive1[Json] =
      onComplete(ctxs.resolve(context)).flatMap {
        case Success(expanded) => {
          provide(expanded deepMerge fs.ctx)
        }
        case Failure(CommandRejected(contextRejection)) =>
          reject(
            ValidationRejection(
              "The context resolution did not succeed",
              Some(IllegalPayload(s"The context resolution did not succeed: $contextRejection", None))))
        case _ =>
          reject(
            MalformedQueryParamRejection(
              "context",
              "IllegalContextExpansion",
              Some(WrongOrInvalidJson(
                Some("The context must be a valid JSON or a URI for context managed in this platform")))))
      }

    parameter('context.as[String] ? Json.obj().noSpaces).flatMap { contextParam =>
      parse(contextParam) match {
        case Right(ctxJson) => resolveContext(Json.obj("@context" -> ctxJson))
        case Left(_)        => resolveContext(Json.obj("@context" -> Json.fromString(contextParam)))
      }
    }
  }

  /**
    * Extracts the ''filter'' query param from the request.
    *
    */
  def filtered(implicit D: Decoder[Filter]): Directive1[Filter] =
    parameter('filter.as[Filter](unmarshaller(toJson)) ? Filter.Empty)

  /**
    * Extracts the ''q'' query param from the request. This param will be used as a full text search
    */
  def q: Directive1[Option[String]] =
    parameter('q.?)

  /**
    * Extracts the ''deprecated'' query param from the request.
    */
  def deprecated: Directive1[Option[Boolean]] =
    parameter('deprecated.as[Boolean].?)

  /**
    * Extracts the ''published'' query param from the request.
    */
  def published: Directive1[Option[Boolean]] =
    parameter('published.as[Boolean].?)

  /**
    * Extracts the ''format'' query param from the request.
    */
  def format(implicit D: Decoder[JsonLdFormat]): Directive1[JsonLdFormat] =
    parameter('format.as[JsonLdFormat](unmarshaller(toJsonString)) ? (JsonLdFormat.Default: JsonLdFormat))

  /**
    * Extracts the ''sort'' query param from the request.
    */
  def sort(implicit D: Decoder[SortList]): Directive1[SortList] =
    parameter('sort.as[SortList](unmarshaller(toJsonArr)) ? SortList.Empty)

  /**
    * Extracts the ''fields'' query param from the request.
    */
  def fields(implicit D: Decoder[Set[Field]]): Directive1[Set[Field]] = {
    parameter('fields.as[Set[Field]](unmarshaller(toJsonArr)) ? Set.empty[Field])
  }

  private def toJsonArr(value: String): Either[Throwable, Json] =
    Right(Json.arr(value.split(",").foldLeft(Vector.empty[Json])((acc, c) => acc :+ Json.fromString(c)): _*))
  private def toJsonString(value: String): Either[Throwable, Json] = Right(Json.fromString(value))
  private def toJson(value: String): Either[Throwable, Json] =
    parse(value).left.map(err => WrongOrInvalidJson(Try(err.message).toOption))

  /**
    * Extracts the [[ch.epfl.bluebrain.nexus.kg.core.queries.Query]] from the provided query parameters.
    *
    * @param qs   the preconfigured query settings
    * @param fs   the preconfigured filtering settings
    * @param ctxs the context operation bundles
    */
  def paramsToQuery(implicit qs: QuerySettings,
                    fs: FilteringSettings,
                    ctxs: Contexts[Future]): Directive[(Pagination, QueryPayload)] =
    context.flatMap { jsonCtx =>
      val queryDec = QueryPayloadDecoder(jsonCtx)
      import queryDec._
      (filtered & q & deprecated & published & fields & sort & paginated).tmap {
        case (filter, query, deprecate, publish, fieldSet, sortList, page) =>
          (page,
           QueryPayload(`@context` = jsonCtx,
                        filter = filter,
                        q = query,
                        deprecated = deprecate,
                        published = publish,
                        fields = fieldSet,
                        sort = sortList))
      }
    }

}

object QueryDirectives extends QueryDirectives
