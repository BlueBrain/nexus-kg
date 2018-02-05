package ch.epfl.bluebrain.nexus.kg.service.query

import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.search.{Sort, SortList}
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations
import ch.epfl.bluebrain.nexus.kg.core.queries.Query.QueryPayload
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Filter
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op.Eq
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.LiteralTerm
import ch.epfl.bluebrain.nexus.kg.core.queries.{Field, JsonLdFormat, QueryResource}
import ch.epfl.bluebrain.nexus.kg.service.routes.{baseUri, filteringSettings, nexusBaseVoc}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.concurrent.Future

class QueryPayloadDecoderSpec
    extends WordSpecLike
    with ScalatestRouteTest
    with Matchers
    with Resources
    with Inspectors {
  private val orgAgg =
    MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
  private val orgs         = Organizations(orgAgg)
  private val domAgg       = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
  private val doms         = Domains(domAgg, orgs)
  private val ctxAgg       = MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval).toF[Future]
  private implicit val ctx = Contexts(ctxAgg, doms, baseUri.toString())

  "A QueryPayloadDecoderSpec" should {

    val queryJson       = jsonContentOf("/query/query-full-text.json")
    val queryFilterJson = jsonContentOf("/query/query-filter-text.json")
    val filter = Filter(
      ComparisonExpr(Eq,
                     UriPath(s"http://www.w3.org/ns/prov#startedAtTime"),
                     LiteralTerm(""""2017-10-07T16:00:00-05:00"""")))
    val list = List(
      queryJson -> QueryPayload(
        q = Some("someText"),
        resource = QueryResource.Schemas,
        deprecated = Some(false),
        published = Some(false),
        format = JsonLdFormat.Expanded,
        fields = Set(Field("all")),
        sort = SortList(List(Sort(s"-${nexusBaseVoc}createdAtTime")))
      ),
      queryFilterJson -> QueryPayload(
        `@context` = Json.obj("some" -> Json.fromString("http://www.w3.org/ns/prov#"),
                              "some" -> Json.fromString("http://example.com/prov#")),
        filter = filter,
        deprecated = Some(true),
        published = Some(true),
        format = JsonLdFormat.Expanded,
        sort = SortList(List(Sort(s"-http://example.com/prov#createdAtTime")))
      )
    )
    "be decoded properly from json" in {
      forAll(list) {
        case (json, model) =>
          QueryPayloadDecoder.resolveContext(json.hcursor.get[Json]("@context").getOrElse(Json.obj())).map { ctx =>
            val decoders = QueryPayloadDecoder(ctx)
            import decoders._
            json.as[QueryPayload] shouldEqual model

          }
      }
    }
  }

}
