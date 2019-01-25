package ch.epfl.bluebrain.nexus.kg.routes

import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import io.circe.Json
import monix.eval.Task

/**
  * Wraps the different clients
  *
  * @param sparql      the sparql indexer client
  * @param elasticSearch     the ElasticSearch indexer client
  * @param adminClient the implicitly available admin client
  * @param iamClient   the implicitly available iam client
  * @param httpClient  the implicitly available [[UntypedHttpClient]]
  * @param uclJson     the implicitly available [[HttpClient]] with a JSON unmarshaller
  * @param mt          the implicitly available [[ActorMaterializer]]
  * @tparam F the monadic effect type
  */
final case class Clients[F[_]](sparql: BlazegraphClient[F])(implicit val elasticSearch: ElasticClient[F],
                                                            val adminClient: AdminClient[F],
                                                            val iamClient: IamClient[F],
                                                            val rsSearch: HttpClient[F, QueryResults[Json]],
                                                            val httpClient: UntypedHttpClient[Task],
                                                            val uclJson: HttpClient[Task, Json],
                                                            val mt: ActorMaterializer)
