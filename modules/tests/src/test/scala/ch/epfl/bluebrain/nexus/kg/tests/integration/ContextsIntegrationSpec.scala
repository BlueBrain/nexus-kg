package ch.epfl.bluebrain.nexus.kg.tests.integration

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextRef}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import ch.epfl.bluebrain.nexus.kg.service.routes.ContextRoutes.ContextConfig
import io.circe.Json
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import org.scalatest.time.{Seconds, Span}

import scala.collection.mutable.Map
import scala.concurrent.ExecutionContextExecutor

class ContextsIntegrationSpec(apiUri: Uri, prefixes: PrefixUris, route: Route)(implicit
                                                                               as: ActorSystem,
                                                                               ec: ExecutionContextExecutor,
                                                                               mt: ActorMaterializer)
    extends BootstrapIntegrationSpec(apiUri, prefixes) {

  import BootstrapIntegrationSpec._
  import contextEncoders._
  implicit val e = deriveEncoder[ContextConfig]

  "A ContextRoutes" when {
    "performing integration tests" should {
      val idsPayload = Map[ContextId, Context]()

      "create contexts successfully" in {
        forAll(contexts) {
          case (contextId, json) =>
            Put(s"/contexts/${contextId.show}", json) ~> addCredentials(ValidCredentials) ~> route ~> check {
              idsPayload += (contextId -> Context(contextId, 2L, json, false, true))
              status shouldEqual StatusCodes.Created
              responseAs[Json] shouldEqual ContextRef(contextId, 1L).asJson
            }
        }
      }
      "publish contexts successfully" in {
        forAll(contexts) {
          case (contextId, _) =>
            Patch(s"/contexts/${contextId.show}/config?rev=1", ContextConfig(published = true)) ~> addCredentials(
              ValidCredentials) ~> route ~> check {
              status shouldEqual StatusCodes.OK
              responseAs[Json] shouldEqual ContextRef(contextId, 2L).asJson
            }
        }
      }

      "list all contexts" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/contexts") ~> addCredentials(ValidCredentials) ~> route ~> check {

            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults = UnscoredQueryResults(contexts.length.toLong, sorted(contexts).take(20).map {
              case (contextId, _) => UnscoredQueryResult(contextId)
            })
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self" -> s"$apiUri/contexts",
                                      "next" -> s"$apiUri/contexts?from=20&size=20")
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "list contexts on organization rand with pagination" in {
        val pagination   = Pagination(0L, 5)
        val path         = s"/contexts/rand?size=${pagination.size}"
        val randContexts = contextsForOrg(contexts, "rand")
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults = UnscoredQueryResults(
              randContexts.length.toLong,
              randContexts
                .map(_._1)
                .map(UnscoredQueryResult(_))
                .take(pagination.size)
            )
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self" -> s"$apiUri$path",
                                      "next" -> s"$apiUri$path&from=5")
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "output the correct total even when the from query parameter is out of scope" in {
        val pagination   = Pagination(0L, 5)
        val path         = s"/contexts/rand?size=${pagination.size}&from=100"
        val randContexts = contextsForOrg(contexts, "rand")
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(randContexts.length.toLong, List.empty[UnscoredQueryResult[ContextId]])
            val expectedLinks = Links(
              "@context" -> s"${prefixes.LinksContext}",
              "self"     -> s"$apiUri$path",
              "previous" -> s"$apiUri$path".replace("from=100", s"from=${randContexts.length - pagination.size}")
            )
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "output the correct total in search in organization nexus when the from query parameter is out of scope" in {
        val size          = 3
        val path          = s"/contexts/nexus?size=$size&from=200"
        val nexusContexts = contextsForOrg(contexts, "nexus")
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(nexusContexts.length.toLong, List.empty[ScoredQueryResult[ContextId]])
          val expectedLinks =
            Links("@context" -> s"${prefixes.LinksContext}",
                  "self"     -> s"$apiUri$path",
                  "previous" -> s"$apiUri$path".replace("from=200", s"from=${nexusContexts.length - size}"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }
      "list contexts on organization nexus and domain development" in {
        val path = s"/contexts/nexus/development"
        val ctxs = contextsForOrgAndDomain(contexts, "nexus", "development")
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(ctxs.length.toLong, ctxs.map { case (contextId, _) => UnscoredQueryResult(contextId) })
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "list contexts on organization nexus and domain development and specific context name" in {
        val (contextId, _) = contexts.head
        val path           = s"/contexts/${contextId.contextName.show}"
        val resultContexts = contextsForOrgAndDomainAndName(contexts,
                                                            contextId.domainId.orgId.id,
                                                            contextId.domainId.id,
                                                            contextId.contextName.show)
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(resultContexts.length.toLong, resultContexts.take(3).map {
              case (id, _) => UnscoredQueryResult(id)
            })
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }
      "deprecate one context on rand organization" in {
        val (contextId, _) = contextsForOrg(contexts, "rand").head
        Delete(s"/contexts/${contextId.show}?rev=2") ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual ContextRef(contextId, 3L).asJson
        }

      }
      "list contexts on organization rand and not deprecated with all fields" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          val path         = s"/contexts/rand?size=3&deprecated=false&fields=all"
          val randContexts = contextsForOrg(contexts, "rand")
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(randContexts.length.toLong - 1L, randContexts.slice(1, 4).map {
                case (id, _) => UnscoredQueryResult(idsPayload(id))
              })
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self" -> s"$apiUri$path",
                                      "next" -> s"$apiUri$path&from=3")
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }
    }
  }

  private def contextsForOrg(ctxs: List[(ContextId, Json)], org: String): List[(ContextId, Json)] =
    ctxs
      .filter(c => c._1.domainId.orgId.id.equals(org))
      .sortWith(_._1.show < _._1.show)

  private def contextsForOrgAndDomain(ctxs: List[(ContextId, Json)],
                                      org: String,
                                      domain: String): List[(ContextId, Json)] =
    ctxs
      .filter(c => c._1.domainId.orgId.id.equals(org) && c._1.domainId.id.equals(domain))
      .sortWith(_._1.show < _._1.show)

  private def contextsForOrgAndDomainAndName(ctxs: List[(ContextId, Json)],
                                             org: String,
                                             domain: String,
                                             name: String): List[(ContextId, Json)] =
    ctxs
      .filter(c =>
        c._1.domainId.orgId.id.equals(org) && c._1.domainId.id.equals(domain) && c._1.contextName.show.equals(name))
      .sortWith(_._1.show < _._1.show)

  private def sorted(ctxs: List[(ContextId, Json)]): List[(ContextId, Json)] = ctxs.sortWith(_._1.show < _._1.show)
}
