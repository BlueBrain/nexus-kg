package ch.epfl.bluebrain.nexus.kg.tests.integration

import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId, DomainRef}
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Json
import io.circe.syntax._
import org.scalatest._
import org.scalatest.time.{Seconds, Span}

import scala.collection.mutable.Map
import scala.concurrent.ExecutionContextExecutor

@DoNotDiscover
class DomainIntegrationSpec(apiUri: Uri, prefixes: PrefixUris, route: Route)(implicit
                                                                             as: ActorSystem,
                                                                             ec: ExecutionContextExecutor,
                                                                             mt: ActorMaterializer)
    extends BootstrapIntegrationSpec(apiUri, prefixes) {

  import BootstrapIntegrationSpec._
  import domsEncoders._

  "A DomainRoutes" when {

    "performing integration tests" should {
      val idsPayload = Map[DomainId, Domain]()

      "create domains successfully" in {
        forAll(domains) {
          case domainId @ DomainId(orgId, name) =>
            val description = s"$name-description"
            val jsonDomain =
              Json.obj("description" -> Json.fromString(description))
            Put(s"/domains/${orgId.id}/${name}", jsonDomain) ~> addCredentials(ValidCredentials) ~> route ~> check {
              idsPayload += (domainId -> Domain(domainId, 1L, false, description))
              status shouldEqual StatusCodes.Created
              responseAs[Json] shouldEqual DomainRef(domainId, 1L).asJson.addCoreContext
            }
        }
      }

      "list all domains" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/domains") ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(domains.size.toLong, domains.take(20).map(UnscoredQueryResult(_)))
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self" -> s"$apiUri/domains",
                                      "next" -> s"$apiUri/domains?from=20&size=20")
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list all domains sorted in creation order" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          val rdfType = PrefixMapping.rdfTypeKey.replace("#", "%23")
          val uri =
            s"/domains?sort=-${prefixes.CoreVocabulary}/createdAtTime,$rdfType,http://localhost/something/that/does/exist"
          Get(uri) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(domains.size.toLong, domains.takeRight(20).reverse.map(UnscoredQueryResult(_)))
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self" -> s"$apiUri$uri",
                                      "next" -> s"$apiUri$uri&from=20&size=20")
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list domains on organization nexus" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/domains/nexus") ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(5L, domains.filter(_.orgId.id == "nexus").map(UnscoredQueryResult(_)))
            val expectedLinks =
              Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri/domains/nexus"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list domains on organization rand with pagination" in {
        val pagination = Pagination(0L, 5)
        val path       = s"/domains/rand?size=${pagination.size}"

        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults = UnscoredQueryResults(randDomains.length.toLong,
                                                       randDomains.map(UnscoredQueryResult(_)).take(pagination.size))
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self" -> s"$apiUri$path",
                                      "next" -> s"$apiUri$path&from=5")
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "output the correct total even when the from query parameter is out of scope" in {
        val pagination = Pagination(0L, 5)
        val path       = s"/domains/rand?size=${pagination.size}&from=100"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(randDomains.length.toLong, List.empty[UnscoredQueryResult[DomainId]])
          val expectedLinks =
            Links("@context" -> s"${prefixes.LinksContext}",
                  "self"     -> s"$apiUri$path",
                  "previous" -> s"$apiUri$path".replace("from=100", s"from=${randDomains.length - 5}"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list domains on organization rand with full text search" in {
        val domainId = domains(7)
        val path     = s"/domains/rand?q=${domainId.id}"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            ScoredQueryResults(1L, 1F, List(ScoredQueryResult(1F, domainId)))
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list domains on organization rand with full text search and all fields retrieved" in {
        val domainId = domains(7)
        val path     = s"/domains/rand?q=${domainId.id}&fields=all"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            ScoredQueryResults(1L, 1F, List(ScoredQueryResult(1F, idsPayload(domainId))))
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "output the correct total in full text search even when the from query parameter is out of scope" in {
        val path = s"/domains/rand?q=${domains(7).id}&size=3&from=200"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            ScoredQueryResults(1L, 1F, List.empty[ScoredQueryResult[DomainId]])
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                    "self"     -> s"$apiUri$path",
                                    "previous" -> s"$apiUri$path".replace("from=200", "from=0"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list domains with filter of description" in {
        val domainId = domains(7)
        val uriFilter = URLEncoder.encode(
          s"""{"@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}, "filter": {"path": "${"description".qualify}", "op": "eq", "value": "${descriptionOf(
            domainId)}"} } """,
          "UTF-8"
        )
        val path = s"/domains/rand?filter=$uriFilter"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(1L, List(UnscoredQueryResult(domainId)))
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "deprecate some domains on nexus organization" in {
        forAll(domains.take(2)) { domainId =>
          Delete(s"/domains/nexus/${domainId.id}?rev=1") ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            responseAs[Json] shouldEqual DomainRef(domainId, 2L).asJson.addCoreContext
          }
        }
      }

      "list domains on organization nexus and deprecated" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/domains/nexus?deprecated=false") ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults = UnscoredQueryResults(3L, domains.slice(2, 5).map(UnscoredQueryResult(_)))
            val expectedLinks =
              Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri/domains/nexus?deprecated=false"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }
    }
  }

  private def descriptionOf(domainId: DomainId): String =
    s"${domainId.id}-description"
}
