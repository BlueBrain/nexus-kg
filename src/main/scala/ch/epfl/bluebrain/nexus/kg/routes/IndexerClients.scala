package ch.epfl.bluebrain.nexus.kg.routes

import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import io.circe.Json

/**
  * Wraps the different indexer clients
  *
  * @param elastic the ElasticSearch indexer client
  * @param sparql  the sparql indexer client
  * @tparam F the monadic effect type
  */
final case class IndexerClients[F[_]](elastic: ElasticClient[F], sparql: BlazegraphClient[F])(
    implicit val rsSearch: HttpClient[F, QueryResults[Json]])
