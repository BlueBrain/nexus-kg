package ch.epfl.bluebrain.nexus.kg.test

import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaRef}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.{DoNotDiscover}
import org.scalatest.time.{Seconds, Span}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.SchemaConfig
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._

import scala.concurrent.ExecutionContextExecutor

@DoNotDiscover
class SchemasIntegrationSpec(apiUri: Uri, route: Route, vocab: Uri)(implicit
  as: ActorSystem, ec: ExecutionContextExecutor, mt: ActorMaterializer)
  extends BootstrapIntegrationSpec(apiUri, vocab) {

  import BootstrapIntegrationSpec._, schemaEncoder._

  "A SchemaRoutes" when {

    "performing integration tests" should {

      "create schemas successfully" in {
        forAll(schemas) { case (schemaId, json) =>
          Put(s"/schemas/${schemaId.show}", json) ~> route ~> check {
            status shouldEqual StatusCodes.Created
            responseAs[Json] shouldEqual SchemaRef(schemaId, 1L).asJson
          }
        }
      }

      "publish schemas successfully" in {
        forAll(schemas) { case (schemaId, _) =>
          Patch(s"/schemas/${schemaId.show}/config?rev=1", SchemaConfig(published = true)) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] shouldEqual SchemaRef(schemaId, 2L).asJson
          }
        }
      }

      "list all schemas" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/schemas") ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults(schemas.length.toLong, schemas.take(20).map{case(schemaId, _) => UnscoredQueryResult(schemaId)})
            val expectedLinks = List(Link("self", s"$apiUri/schemas"), Link("next", s"$apiUri/schemas?from=20&size=20"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "list schemas on organization rand with pagination" in {
        val pagination = Pagination(0L, 5)
        val path = s"/schemas/rand?size=${pagination.size}"
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults((schemas.length - 3 * 5).toLong, schemas.collect { case (schemaId@SchemaId(DomainId(orgId, _), _, _), _) if orgId.id == "rand" => schemaId }.map(UnscoredQueryResult(_)).take(pagination.size))
            val expectedLinks = List(Link("self", s"$apiUri$path"), Link("next", s"$apiUri$path&from=5"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "output the correct total even when the from query parameter is out of scope" in {
        val pagination = Pagination(0L, 5)
        val path = s"/schemas/rand?size=${pagination.size}&from=100"
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults((schemas.length - 3 * 5).toLong, List.empty[UnscoredQueryResult[SchemaId]])
            val expectedLinks = List(Link("self", s"$apiUri$path"), Link("previous", s"$apiUri$path".replace("from=100", s"from=${(schemas.length - 3 * 5) - 5}")))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "list schemas on organization nexus with full text search" in {
        val (schemaId, _) = schemas.head
        val path = s"/schemas/nexus?q=${schemaId.name}"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults = ScoredQueryResults(3L, 1F, schemas.take(3).map{ case(id, _) => ScoredQueryResult(1F, id)})
          val expectedLinks = List(Link("self", s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "output the correct total in full text search even when the from query parameter is out of scope" in {
        val (schemaId, _) = schemas.head
        val path = s"/schemas/nexus?q=${schemaId.name}&size=3&from=200"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults = ScoredQueryResults(3L, 1F, List.empty[ScoredQueryResult[SchemaId]])
          val expectedLinks = List(Link("self", s"$apiUri$path"), Link("previous", s"$apiUri$path".replace("from=200", "from=0")))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "list schemas on organization nexus and domain development" in {
        val path = s"/schemas/nexus/development"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults = UnscoredQueryResults(3L, schemas.take(3).map{ case(schemaId, _) => UnscoredQueryResult(schemaId)})
          val expectedLinks = List(Link("self", s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "list schemas on organization nexus and domain development and specific schema name" in {
        val (schemaId, _) = schemas.head
        val path = s"/schemas/${schemaId.schemaName.show}"
        Get(path) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          val expectedResults = UnscoredQueryResults(3L, schemas.take(3).map{ case(id, _) => UnscoredQueryResult(id)})
          val expectedLinks = List(Link("self", s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
        }
      }

      "list schemas with filter of type owl:Ontology1" in {
        val uriFilter = URLEncoder.encode(s"""{"@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "owl": "http://www.w3.org/2002/07/owl#"}, "filter": {"path": "rdf:type", "op": "eq", "value": "owl:Ontology1"} }""", "UTF-8")
        val path = s"/schemas/rand?filter=$uriFilter&size=10"
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults(randDomains.length.toLong, schemas
              .collect { case (schemaId@SchemaId(DomainId(orgId, _), _, _), json) if orgId.id == "rand" && json.hcursor.get[String]("@type").toOption.contains("owl:Ontology1") => schemaId }.map(UnscoredQueryResult(_)).take(10))
            val uri = Uri(s"$apiUri$path")
            val expectedLinks = List(Link("self", uri), Link("next", uri.withQuery(Query(uri.query().toMap + ("from" -> "10") + ("size" -> "10")))))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }

      "deprecate one schemas on nexus organization" in {
        val (schemaId, _) = schemas.head
        Delete(s"/schemas/${schemaId.show}?rev=2") ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual SchemaRef(schemaId, 3L).asJson
        }
      }

      "list schemas on organizations rand and deprecated" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          val (schemaId, _) = schemas.head
          val path = s"/schemas/${schemaId.schemaName.show}?deprecated=false"
          Get(path) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            val expectedResults = UnscoredQueryResults(2L, schemas.slice(1, 3).map{ case(id, _) => UnscoredQueryResult(id)})
            val expectedLinks = List(Link("self", s"$apiUri$path"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson
          }
        }
      }
    }
  }
}
