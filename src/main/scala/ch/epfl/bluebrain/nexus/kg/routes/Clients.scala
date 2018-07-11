package ch.epfl.bluebrain.nexus.kg.routes

import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import io.circe.Json

/**
  * Wraps the different clients
  *
  * @param elastic     the ElasticSearch indexer client
  * @param sparql      the sparql indexer client
  * @param adminClient the implicitly available admin client
  * @param iamClient   the implicitly available iam client
  * @tparam F the monadic effect type
  */
final case class Clients[F[_]](elastic: ElasticClient[F], sparql: BlazegraphClient[F])(
    implicit val adminClient: AdminClient[F],
    val iamClient: IamClient[F],
    val rsSearch: HttpClient[F, QueryResults[Json]])
