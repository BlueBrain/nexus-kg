package ch.epfl.bluebrain.nexus.kg

import cats.data.EitherT
import cats.effect.{Effect, Timer}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticSearchServerOrUnexpectedFailure
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.iam.client.types.Caller
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileAttributes
import ch.epfl.bluebrain.nexus.kg.routes.SearchParams
import ch.epfl.bluebrain.nexus.kg.search.QueryBuilder.queryFor
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._
import io.circe.Json

package object resources {

  /**
    * A resource id rooted in a project reference.
    */
  type ResId = Id[ProjectRef]

  /**
    * Primary resource representation.
    */
  type Resource = ResourceF[Json]

  /**
    * Resource representation with a "source", "flattened" context and "computed" graph.
    */
  type ResourceV = ResourceF[ResourceF.Value]

  /**
    * Resource tags
    */
  type TagSet = Set[Tag]

  /**
    * Rejection or resource representation with a "source", "flattened" context and "computed" graph wrapped in F[_]
    */
  type RejOrResourceV[F[_]] = EitherT[F, Rejection, ResourceV]

  /**
    * Rejection or resource representation with a "source" wrapped in F[_]
    */
  type RejOrResource[F[_]] = EitherT[F, Rejection, Resource]

  /**
    * Rejection or tags representation wrapped in F[_]
    */
  type RejOrTags[F[_]] = EitherT[F, Rejection, TagSet]

  /**
    * Rejection or Unit wrapped in F[_]
    */
  type RejOrUnit[F[_]] = EitherT[F, Rejection, Unit]

  /**
    * Rejection or file representation containing the storage, the file attributes and the Source wrapped in F[_]
    */
  type RejOrFile[F[_], Out] = EitherT[F, Rejection, (Storage, FileAttributes, Out)]

  /**
    * Query results of type Json
    */
  type JsonResults = QueryResults[Json]

  implicit def toSubject(implicit caller: Caller): Subject = caller.subject

  private[resources] def listResources[F[_]: Timer](view: Option[ElasticSearchView],
                                                    params: SearchParams,
                                                    pagination: Pagination)(
      implicit F: Effect[F],
      config: AppConfig,
      tc: HttpClient[F, JsonResults],
      elasticSearch: ElasticSearchClient[F]): F[JsonResults] = {
    import ch.epfl.bluebrain.nexus.kg.instances.elasticErrorMonadError
    implicit val retryer = Retry[F, ElasticSearchServerOrUnexpectedFailure](config.elasticSearch.query.retryStrategy)

    view
      .map(v => elasticSearch.search[Json](queryFor(params), Set(v.index))(pagination))
      .getOrElse(F.pure[JsonResults](UnscoredQueryResults(0L, List.empty)))
      .retry
  }

}
