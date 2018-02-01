package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.types.search.Sort.OrderType
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, Sort, SortList}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Filter
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.service.prefixes.ErrorContext
import ch.epfl.bluebrain.nexus.kg.service.routes.{
  Error,
  ExceptionHandling,
  RejectionHandling,
  baseUri,
  contextJson,
  filteringSettings
}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.generic.auto._
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future
class QueryDirectivesSpec extends WordSpecLike with ScalatestRouteTest with Matchers {

  private case class Response(pagination: Pagination,
                              qOpt: Option[String],
                              deprecatedOpt: Option[Boolean],
                              fields: Set[String],
                              sort: SortList)

  implicit val filterDecoder = Filter.filterDecoder(contextJson)

  private val orgAgg =
    MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
  private val orgs         = Organizations(orgAgg)
  private val domAgg       = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
  private val doms         = Domains(domAgg, orgs)
  private val ctxAgg       = MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval).toF[Future]
  private implicit val ctx = Contexts(ctxAgg, doms, baseUri.toString())

  private def route(implicit qs: QuerySettings) = {
    (handleExceptions(ExceptionHandling.exceptionHandler(ErrorContext)) & handleRejections(
      RejectionHandling.rejectionHandler(ErrorContext))) {
      (get & searchQueryParams) { (pagination, _, qOpt, deprecatedOpt, fields, sort) =>
        complete(Response(pagination, qOpt, deprecatedOpt, fields, sort))
      }
    }
  }

  "A searchQueryParams directive" should {
    val base = "http://localhost"
    implicit val qs =
      QuerySettings(Pagination(0, 20), 100, "index", filteringSettings.nexusBaseVoc, base, s"$base/acls/graph")

    "extract default page when not provided" in {
      Get("/") ~> route ~> check {
        responseAs[Response] shouldEqual Response(qs.pagination, None, None, Set.empty, SortList.Empty)
      }
    }

    "extract provided page" in {
      Get("/?from=1&size=30") ~> route ~> check {
        responseAs[Response] shouldEqual Response(Pagination(1L, 30), None, None, Set.empty, SortList.Empty)
      }
    }

    "extract 0 when size and from are negative" in {
      Get("/?from=-1&size=-30") ~> route ~> check {
        responseAs[Response] shouldEqual Response(Pagination(0L, 1), None, None, Set.empty, SortList.Empty)
      }
    }

    "extract maximum page size when provided is greater" in {
      Get("/?from=1&size=300") ~> route ~> check {
        responseAs[Response] shouldEqual Response(Pagination(1L, 100), None, None, Set.empty, SortList.Empty)
      }
    }

    "extract deprecated and q query params when provided" in {
      Get("/?deprecated=false&q=something") ~> route ~> check {
        responseAs[Response] shouldEqual Response(qs.pagination,
                                                  Some("something"),
                                                  Some(false),
                                                  Set.empty,
                                                  SortList.Empty)
      }
    }

    "extract fields, pagination, q and deprecated when provided" in {
      Get("/?deprecated=true&q=something&from=1&size=30&fields=one,two,three,,") ~> route ~> check {
        responseAs[Response] shouldEqual Response(Pagination(1L, 30),
                                                  Some("something"),
                                                  Some(true),
                                                  Set("one", "two", "three"),
                                                  SortList.Empty)
      }
    }
    "extract sort when provided" in {
      val rdfType = PrefixMapping.rdfTypeKey.replace("#", "%23")
      Get(s"/?sort=$base/createdAtTime,${rdfType},,,") ~> route ~> check {
        val expectedSort =
          SortList(List(Sort(OrderType.Asc, s"$base/createdAtTime"), Sort(OrderType.Asc, PrefixMapping.rdfTypeKey)))
        responseAs[Response] shouldEqual Response(qs.pagination, None, None, Set.empty, expectedSort)
      }
    }
  }

  "A format query param" should {
    val resourceRoute = (handleExceptions(ExceptionHandling.exceptionHandler(ErrorContext)) &
      handleRejections(RejectionHandling.rejectionHandler(ErrorContext))) {
      (get & format) { f =>
        complete(f)
      }
    }

    "extract the JSON-LD output format when provided" in {
      Get("/?format=expanded") ~> resourceRoute ~> check {
        responseAs[JsonLdFormat] shouldEqual JsonLdFormat.Expanded
      }
      Get("/?format=flattened") ~> resourceRoute ~> check {
        responseAs[JsonLdFormat] shouldEqual JsonLdFormat.Flattened
      }
      Get("/?format=compacted") ~> resourceRoute ~> check {
        responseAs[JsonLdFormat] shouldEqual JsonLdFormat.Compacted
      }
    }

    "fall back to the default output format when absent" in {
      Get("/") ~> resourceRoute ~> check {
        responseAs[JsonLdFormat] shouldEqual JsonLdFormat.Default
      }
    }

    "reject when an invalid JSON-LD output format is provided" in {
      Get("/?format=foobar") ~> resourceRoute ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error] shouldEqual Error("IllegalOutputFormat",
                                            Some("Unsupported JSON-LD output formats: 'foobar'"),
                                            "http://localhost/v0/contexts/nexus/core/error/v0.1.0")
      }
    }
  }

}
