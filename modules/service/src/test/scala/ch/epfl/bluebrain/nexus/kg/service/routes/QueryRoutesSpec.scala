package ch.epfl.bluebrain.nexus.kg.service.routes

import java.time.Clock

import akka.cluster.Cluster
import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.TestKit
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidator
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, Sort, SortList}
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.cache.ShardedCache.CacheSettings
import ch.epfl.bluebrain.nexus.kg.core.cache.{Cache, ShardedCache}
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains
import ch.epfl.bluebrain.nexus.kg.core.instances.Instances
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations
import ch.epfl.bluebrain.nexus.kg.core.queries.Query.QueryPayload
import ch.epfl.bluebrain.nexus.kg.core.queries._
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Filter
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op.Eq
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.LiteralTerm
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaImportResolver, Schemas}
import ch.epfl.bluebrain.nexus.kg.indexing.instances.InstanceSparqlIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.BootstrapService.iamClient
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.instances.attachments.{AkkaInOutFileStream, RelativeAttachmentLocation}
import ch.epfl.bluebrain.nexus.kg.service.io.BaseEncoder
import ch.epfl.bluebrain.nexus.kg.service.prefixes
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalVersionFormat
import ch.epfl.bluebrain.nexus.kg.service.routes.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.service.routes.encoders.GroupedIdsToEntityRetrieval
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.circe.generic.auto._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Inspectors, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

//noinspection TypeAnnotation
class QueryRoutesSpec
    extends WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with Randomness
    with Resources
    with ScalaFutures
    with Inspectors
    with MockedIAMClient
    with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 100 millis)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).join(Cluster(system).selfAddress)
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  val settings  = new Settings(ConfigFactory.load())
  val algorithm = settings.Attachment.HashAlgorithm

  val orgAgg                                          = MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
  val orgs                                            = Organizations(orgAgg)
  val domAgg                                          = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
  val doms                                            = Domains(domAgg, orgs)
  val schAgg                                          = MemoryAggregate("schemas")(Schemas.initial, Schemas.next, Schemas.eval).toF[Future]
  val ctxAgg                                          = MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval).toF[Future]
  implicit val contexts                               = Contexts(ctxAgg, doms, baseUri.toString())
  val schemas                                         = Schemas(schAgg, doms, contexts, baseUri.toString())
  val validator                                       = ShaclValidator[Future](SchemaImportResolver(baseUri.toString(), schemas.fetch))
  val instAgg                                         = MemoryAggregate("instances")(Instances.initial, Instances.next, Instances.eval).toF[Future]
  implicit val fa: RelativeAttachmentLocation[Future] = RelativeAttachmentLocation[Future](settings)
  val inFileProcessor                                 = AkkaInOutFileStream(settings)
  val instances                                       = Instances(instAgg, schemas, contexts, validator, inFileProcessor)

  val groupedIds                  = new GroupedIdsToEntityRetrieval(instances, schemas, contexts, doms, orgs)
  val cache: Cache[Future, Query] = ShardedCache[Query]("some", CacheSettings())
  val queries                     = Queries(cache)

  implicit val clock = Clock.systemUTC
  val caller         = CallerCtx(clock, AnonymousCaller(Anonymous()))

  private val InstanceSparqlIndexingSettings(index, _, _, nexusVocBase) =
    InstanceSparqlIndexingSettings(genString(length = 6), baseUri, s"$baseUri/data/graphs", s"$baseUri/voc/nexus/core")

  val querySettings = QuerySettings(Pagination(0L, 20), 100, index, nexusVocBase, baseUri, s"$baseUri/acls/graph")

  val sparqlUri = Uri("http://localhost:9999/bigdata/sparql")

  val client = SparqlClient[Future](sparqlUri)

  implicit val cl = iamClient("http://localhost:8080")

  val route = QueryRoutes(queries, client, querySettings, groupedIds, baseUri).routes

  val baseEncoder = new BaseEncoder(prefixes)

  "An QueryRoutes" should {

    val queryJson       = jsonContentOf("/query/query-full-text.json")
    val queryJsonFilter = jsonContentOf("/query/query-filter-text.json")

    val queryExpandedContext = jsonContentOf("/query/query-context.json")

    val queryModel = QueryPayload(
      `@context` = queryExpandedContext,
      q = Some("someText"),
      resource = QueryResource.Schemas,
      deprecated = Some(false),
      published = Some(false),
      format = JsonLdFormat.Expanded,
      fields = Set(Field("all")),
      sort = SortList(List(Sort(s"-${nexusBaseVoc}createdAtTime")))
    )

    val filter = Filter(
      ComparisonExpr(Eq,
                     UriPath(s"http://www.w3.org/ns/prov#startedAtTime"),
                     LiteralTerm(""""2017-10-07T16:00:00-05:00"""")))

    val queryModelFilter = QueryPayload(
      `@context` = Json.obj("some" -> Json.fromString("http://example.com/prov#")) deepMerge queryExpandedContext,
      filter = filter,
      deprecated = Some(true),
      published = Some(true),
      format = JsonLdFormat.Compacted,
      sort = SortList(List(Sort(s"-http://example.com/prov#createdAtTime")))
    )

    "store a query" in {
      Post("/queries/org/dom", queryJson) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.PermanentRedirect
        val location = header("Location").get.value()
        location should startWith(s"$baseUri/queries/")
        location should endWith(s"?from=${querySettings.pagination.from}&size=${querySettings.pagination.size}")
        val queryId = QueryId(
          location
            .replace(s"$baseUri/queries/", "")
            .replace(s"?from=${querySettings.pagination.from}&size=${querySettings.pagination.size}", ""))
        queries.fetch(queryId).futureValue.get shouldEqual Query(queryId, "org" / "dom", queryModel)
      }
    }

    "store a query with wrong path and follow redirect" in {
      Post("/queries/org/dom/name/1.2.3?from=1&size=10", queryJsonFilter) ~> addCredentials(ValidCredentials) ~> route ~> check {
        status shouldEqual StatusCodes.PermanentRedirect
        val location = header("Location").get.value()
        location should startWith(s"$baseUri/queries/")
        location should endWith(s"?from=1&size=10")
        val queryId = QueryId(
          location
            .replace(s"$baseUri/queries/", "")
            .replace(s"?from=1&size=10", ""))
        queries.fetch(queryId).futureValue.get shouldEqual Query(queryId,
                                                                 "org" / "dom" / "name" / "1.2.3",
                                                                 queryModelFilter)
        Get(location.replace(baseUri.toString, "")) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[Error].code shouldEqual classNameOf[IllegalVersionFormat.type]
        }

      }
    }

  }
}
