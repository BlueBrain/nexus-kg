package ch.epfl.bluebrain.nexus.kg.tests.integration

import java.net.URLEncoder
import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId, DomainRef}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Json
import io.circe.syntax._
import org.scalatest._
import org.scalatest.time.{Seconds, Span}
import Pattern.quote

import scala.collection.mutable.Map
import scala.concurrent.ExecutionContextExecutor

@DoNotDiscover
class DomainIntegrationSpec(apiUri: Uri, prefixes: PrefixUris, route: Route)(implicit
                                                                             as: ActorSystem,
                                                                             ec: ExecutionContextExecutor,
                                                                             mt: ActorMaterializer)
    extends BootstrapIntegrationSpec(apiUri, prefixes)
    with Resources {

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

      "list all domains sorted in creation order" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          val rdfType = PrefixMapping.rdfTypeKey.replace("#", "%23")
          val simpleCtx =
            URLEncoder.encode(Json.obj("nxx" -> Json.fromString(prefixes.CoreVocabulary.toString)).noSpaces, "UTF-8")
          val uris = List(
            s"/domains?sort=-${prefixes.CoreVocabulary}createdAtTime,$rdfType,http://localhost/something/that/does/exist",
            "/domains?sort=-nxv:createdAtTime,rdf:type",
            s"/domains?context=$simpleCtx&sort=-nxx:createdAtTime,rdf:type"
          )
          forAll(uris) {
            case uri =>
              Get(uri) ~> addCredentials(ValidCredentials) ~> route ~> check {
                status shouldEqual StatusCodes.OK
                contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
                val expectedResults =
                  UnscoredQueryResults(domains.size.toLong, domains.takeRight(10).reverse.map(UnscoredQueryResult(_)))
                val expectedLinks = Links(
                  "@context" -> s"${prefixes.LinksContext}",
                  "self"     -> s"$apiUri$uri",
                  "next" -> s"$apiUri$uri&from=10&size=10"
                    .replaceAll(quote("%3A"), ":")
                    .replaceAll(quote("%2F"), "/")
                )
                contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
                responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
              }
          }

        }
      }

      "list all domains sorted in creation order using stored query" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          val queries = List(
            jsonContentOf(
              "/query/query-sort.json",
              scala.collection.immutable.Map(quote("{{CoreVocabulary}}") -> prefixes.CoreVocabulary.toString)),
            jsonContentOf(
              "/query/query-sort-context.json",
              scala.collection.immutable.Map(quote("{{CoreVocabulary}}") -> prefixes.CoreVocabulary.toString))
          )
          forAll(queries) { query =>
            Post("/queries", query) ~> addCredentials(ValidCredentials) ~> route ~> check {
              status shouldEqual StatusCodes.PermanentRedirect
              val location = header("Location").get.value()
              location should startWith(s"$apiUri/queries/")
              location should endWith(s"?from=0&size=10")
              val path = location.replace(apiUri.toString, "")
              Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
                status shouldEqual StatusCodes.OK
                contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
                val expectedResults =
                  UnscoredQueryResults(domains.size.toLong, domains.takeRight(10).reverse.map(UnscoredQueryResult(_)))
                val expectedLinks = Links(
                  "@context" -> s"${prefixes.LinksContext}",
                  "self"     -> s"$apiUri$path",
                  "next"     -> s"""$apiUri${path.replace("from=0", "from=10")}"""
                )
                contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
                responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
              }
            }
          }
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
        val domainId   = domains(7)
        val uriContext = URLEncoder.encode("""{"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}""", "UTF-8")
        val uriFilter = URLEncoder.encode(
          s"""{"path": "${"description".qualify}", "op": "eq", "value": "${descriptionOf(domainId)}"}""",
          "UTF-8")
        val path = s"/domains/rand?context=$uriContext&filter=$uriFilter"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(1L, List(UnscoredQueryResult(domainId)))
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list domains with filter of description using stored query" in {
        val domainId = domains(7)
        val query = jsonContentOf(
          "/query/query-filter.json",
          scala.collection.immutable.Map(quote("{{description_path}}")  -> "description".qualifyAsString,
                                         quote("{{description_value}}") -> descriptionOf(domainId))
        )
        Post("/queries/rand", query) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.PermanentRedirect
          val location = header("Location").get.value()
          location should startWith(s"$apiUri/queries/")
          location should endWith(s"?from=0&size=10")
          val path = location.replace(apiUri.toString, "")
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(1L, List(UnscoredQueryResult(domainId)))
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
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

    }
  }

  private def descriptionOf(domainId: DomainId): String =
    s"${domainId.id}-description"
}
