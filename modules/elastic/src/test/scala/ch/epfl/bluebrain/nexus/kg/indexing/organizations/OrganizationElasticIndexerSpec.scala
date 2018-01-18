package ch.epfl.bluebrain.nexus.kg.indexing.organizations

import java.time.Clock

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder, ElasticQueryClient}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.ld.JsonLdOps._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import io.circe.{Decoder, Json}
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexingSettings, IndexerFixture}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
@DoNotDiscover
class OrganizationElasticIndexerSpec(esUri: Uri)
    extends TestKit(ActorSystem("OrganizationElasticIndexerSpec"))
    with IndexerFixture
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Randomness
    with Resources
    with Inspectors
    with BeforeAndAfterAll
    with Eventually
    with CancelAfterFailure
    with Assertions {

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(3 seconds, 100 milliseconds)

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val mt: ActorMaterializer        = ActorMaterializer()

  private implicit val cl: UntypedHttpClient[Future]                    = HttpClient.akkaHttpClient
  private implicit val D: Decoder[QueryResults[Json]]                   = ElasticDecoder[Json]
  private implicit val rsSearch: HttpClient[Future, QueryResults[Json]] = withAkkaUnmarshaller[QueryResults[Json]]
  private val client                                                    = ElasticClient[Future](esUri, ElasticQueryClient[Future](esUri))

  private val base = s"http://$localhost/v0"

  private val settings @ ElasticIndexingSettings(indexPrefix, _, orgBase, nexusVocBase) =
    ElasticIndexingSettings(genString(length = 6), genString(length = 6), base, s"$base/voc/nexus/core")
  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](nexusVocBase)

  private def getAll: Future[QueryResults[Json]] =
    client.search[Json](Json.obj("query" -> Json.obj("match_all" -> Json.obj())))(Pagination(0, 100))

  private def expectedJson(id: OrgId, rev: Long, deprecated: Boolean, meta: Meta, firstReqMeta: Meta): Json = {
    Json.obj(
      createdAtTimeKey                               -> firstReqMeta.instant.jsonLd,
      idKey                                          -> Json.fromString(id.qualifyAsStringWith(orgBase)),
      "rev".qualifyAsStringWith(nexusVocBase)        -> Json.fromLong(rev),
      "name".qualifyAsStringWith(nexusVocBase)       -> Json.fromString(id.id),
      updatedAtTimeKey                               -> meta.instant.jsonLd,
      rdfTypeKey                                     -> "Organization".qualify.jsonLd,
      "deprecated".qualifyAsStringWith(nexusVocBase) -> Json.fromBoolean(deprecated)
    )
  }

  "A OrganizationElasticIndexer" should {

    val (ctxs, replacements) = createContext(base)

    val indexer = OrganizationElasticIndexer(client, ctxs, settings)

    val id   = OrgId(genString(length = 4))
    val meta = Meta(Anonymous(), Clock.systemUTC.instant())

    "index a OrgCreated event" in {
      val indexId = id.toIndex(indexPrefix)
      whenReady(client.existsIndex(indexId).failed) { e =>
        e shouldBe a[ElasticClientError]
      }
      val rev  = 1L
      val data = jsonContentOf("/instances/minimal.json", replacements)
      indexer(OrgCreated(id, rev, meta, data)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json.deepMerge(expectedJson(id, rev, false, meta, meta))
        client.existsIndex(indexId).futureValue shouldEqual (())
      }
    }
    val dataUpdated = jsonContentOf("/instances/minimal.json", replacements + ("random" -> "updated"))
    "index a OrgUpdated event" in {
      val metaUpdate = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev        = 2L
      indexer(OrgUpdated(id, rev, metaUpdate, dataUpdated)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(dataUpdated).futureValue
        rs.results.head.source shouldEqual json.deepMerge(expectedJson(id, rev, false, metaUpdate, meta))
      }
    }

    "index a OrgDeprecated event" in {
      val metaUpdate = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev        = 3L
      indexer(OrgDeprecated(id, rev, metaUpdate)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(dataUpdated).futureValue
        rs.results.head.source shouldEqual json.deepMerge(expectedJson(id, rev, true, metaUpdate, meta))
      }
    }
  }
}
