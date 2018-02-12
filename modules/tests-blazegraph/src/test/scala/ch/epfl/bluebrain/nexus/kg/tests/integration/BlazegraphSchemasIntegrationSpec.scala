package ch.epfl.bluebrain.nexus.kg.tests.integration

import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId, SchemaRef}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.SchemaConfig
import io.circe.Json
import io.circe.syntax._
import org.scalatest.DoNotDiscover
import org.scalatest.time.{Seconds, Span}

import scala.collection.mutable.Map
import scala.concurrent.ExecutionContextExecutor

@DoNotDiscover
class BlazegraphSchemasIntegrationSpec(apiUri: Uri, prefixes: PrefixUris, route: Route)(implicit
                                                                                        as: ActorSystem,
                                                                                        ec: ExecutionContextExecutor,
                                                                                        mt: ActorMaterializer)
    extends BootstrapIntegrationSpec(apiUri, prefixes) {

  import BootstrapIntegrationSpec._
  import schemaEncoders._

  "A SchemaRoutes" when {
    val idsPayload = Map[SchemaId, Schema]()

    "performing integration tests" should {

      "create schemas successfully" in {
        forAll(schemas) {
          case (schemaId, json) =>
            Put(s"/schemas/${schemaId.show}", json) ~> addCredentials(ValidCredentials) ~> route ~> check {
              idsPayload += (schemaId -> Schema(schemaId, 2L, json, false, true))
              status shouldEqual StatusCodes.Created
              responseAs[Json] shouldEqual SchemaRef(schemaId, 1L).asJson.addCoreContext
            }
        }
      }

      "publish schemas successfully" in {
        forAll(schemas) {
          case (schemaId, _) =>
            Patch(s"/schemas/${schemaId.show}/config?rev=1", SchemaConfig(published = true)) ~> addCredentials(
              ValidCredentials) ~> route ~> check {
              status shouldEqual StatusCodes.OK
              responseAs[Json] shouldEqual SchemaRef(schemaId, 2L).asJson.addCoreContext
            }
        }
      }

      "list schemas on organization nexus with full text search" in {
        val (schemaId, _) = schemas.head
        val path          = s"/schemas/nexus?q=${schemaId.name}"
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              ScoredQueryResults(3L, 1F, schemas.take(3).map { case (id, _) => ScoredQueryResult(1F, id) })
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "output the correct total in full text search even when the from query parameter is out of scope" in {
        val (schemaId, _) = schemas.head
        val path          = s"/schemas/nexus?q=${schemaId.name}&size=3&from=200"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults = ScoredQueryResults(3L, 1F, List.empty[ScoredQueryResult[SchemaId]])
          val expectedLinks =
            Links("@context" -> s"${prefixes.LinksContext}",
                  "self"     -> s"$apiUri$path",
                  "previous" -> s"$apiUri$path".replace("from=200", "from=0"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list schemas with filter of type owl:Ontology1" in {
        val uriContext = URLEncoder.encode(
          """{"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "owl": "http://www.w3.org/2002/07/owl#"}""",
          "UTF-8")
        val uriFilter = URLEncoder.encode(s"""{"path": "rdf:type", "op": "eq", "value": "owl:Ontology1"}""", "UTF-8")
        val path      = s"/schemas/rand?context=$uriContext&filter=$uriFilter&size=10"
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults(
              randDomains.length.toLong,
              schemas
                .collect {
                  case (schemaId @ SchemaId(DomainId(orgId, _), _, _), json)
                      if orgId.id == "rand" && json.hcursor.get[String]("@type").toOption.contains("owl:Ontology1") =>
                    schemaId
                }
                .map(UnscoredQueryResult(_))
                .take(10)
            )
            val uri = Uri(s"$apiUri$path")
            val expectedLinks =
              Links("@context" -> s"${prefixes.LinksContext}",
                    "self"     -> uri,
                    "next"     -> uri.withQuery(Query(uri.query().toMap + ("from" -> "10") + ("size" -> "10"))))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "deprecate one schemas on nexus organization" in {
        val (schemaId, _) = schemas.head
        Delete(s"/schemas/${schemaId.show}?rev=2") ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual SchemaRef(schemaId, 3L).asJson.addCoreContext
        }
      }

    }
  }
}
