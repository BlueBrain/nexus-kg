package ch.epfl.bluebrain.nexus.kg.service.routes

import java.net.URLEncoder
import java.time.Clock

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection._
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.prefixes
import ch.epfl.bluebrain.nexus.kg.service.BootstrapService.iamClient
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections._
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future

class RejectionHandlingSpec
    extends WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with Randomness
    with ScalaFutures
    with MockedIAMClient {

  "A RejectionHandling" should {
    val baseUri                    = Uri("http://localhost/v0")
    val orgAgg                     = MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
    val orgs                       = Organizations(orgAgg)
    val id                         = genString(length = 5)
    val nexusVocab                 = s"$baseUri/voc/nexus/core"
    implicit val filteringSettings = FilteringSettings(nexusVocab, nexusVocab)
    implicit val cl                = iamClient("http://localhost:8080")
    implicit val clock             = Clock.systemUTC

    val sparqlUri     = Uri("http://localhost:9999/bigdata/sparql")
    val vocab         = baseUri.copy(path = baseUri.path / "core")
    val querySettings = QuerySettings(Pagination(0L, 20), 100, "org-index", vocab, baseUri, s"$baseUri/acls/graph")

    val sparqlClient = SparqlClient[Future](sparqlUri)
    val route =
      OrganizationRoutes(orgs, sparqlClient, querySettings, baseUri, prefixes).routes

    "reject the creation of a organization with invalid JSON payload" in {
      val invalidJson =
        HttpEntity(ContentTypes.`application/json`, s"""{"key" "value"}""")
      Put(s"/organizations/$id", invalidJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[WrongOrInvalidJson.type]
      }
    }

    "reject the request of an verb not allowed for a particular resource" in {
      Head(s"/organizations/$id") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.MethodNotAllowed
        responseAs[Error].code shouldEqual classNameOf[MethodNotSupported.type]
        responseAs[MethodNotSupported].supported should contain theSameElementsAs Vector("GET", "DELETE", "PUT")

      }
    }

    "reject the request with a filter which has the wrong format" in {
      val filter = URLEncoder.encode(s"""{"a": "b"}""", "UTF-8")
      Get(s"/organizations?filter=$filter") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalFilterFormat.type]
      }
    }

    "reject the request with a filter which is not JSON format" in {
      Get(s"/organizations?filter=wrong") ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[WrongOrInvalidJson.type]
      }
    }
  }
}
