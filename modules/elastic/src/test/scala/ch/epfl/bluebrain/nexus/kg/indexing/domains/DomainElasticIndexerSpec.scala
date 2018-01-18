package ch.epfl.bluebrain.nexus.kg.indexing.domains

import java.time.Clock

import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder, ElasticQueryClient}
import ch.epfl.bluebrain.nexus.commons.es.server.embed.ElasticServer
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent.{DomainCreated, DomainDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.ld.JsonLdOps._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexingSettings, IndexerFixture}
import io.circe.{Decoder, Json}
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._

class DomainElasticIndexerSpec
  extends ElasticServer
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
    PatienceConfig(6 seconds, 300 milliseconds)


  private implicit val cl: UntypedHttpClient[Future]                    = HttpClient.akkaHttpClient
  private implicit val D: Decoder[QueryResults[Json]]                   = ElasticDecoder[Json]
  private implicit val rsSearch: HttpClient[Future, QueryResults[Json]] = withAkkaUnmarshaller[QueryResults[Json]]
  private val client                                                    = ElasticClient[Future](esUri, ElasticQueryClient[Future](esUri))

  private val base = s"http://$localhost/v0"

  private val settings @ ElasticIndexingSettings(indexPrefix, _, orgBase, nexusVocBase) =
    ElasticIndexingSettings(genString(length = 6), genString(length = 6), base, s"$base/voc/nexus/core")
  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](nexusVocBase)
  implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)

  private def getAll: Future[QueryResults[Json]] =
    client.search[Json](Json.obj("query" -> Json.obj("match_all" -> Json.obj())))(Pagination(0, 100))

  private def expectedJson(id: DomainId,
                           rev: Long,
                           description: String,
                           deprecated: Boolean,
                           meta: Meta,
                           firstReqMeta: Meta): Json = {
    Json.obj(
      createdAtTimeKey                                 -> firstReqMeta.instant.jsonLd,
      idKey                                            -> Json.fromString(id.qualifyAsStringWith(orgBase)),
      "rev".qualifyAsStringWith(nexusVocBase)          -> Json.fromLong(rev),
      "organization".qualifyAsStringWith(nexusVocBase) -> id.orgId.qualify.jsonLd,
      "name".qualifyAsStringWith(nexusVocBase)         -> Json.fromString(id.id),
      "description".qualifyAsStringWith(nexusVocBase)  -> Json.fromString(description),
      updatedAtTimeKey                                 -> meta.instant.jsonLd,
      rdfTypeKey                                       -> "Domain".qualify.jsonLd,
      "deprecated".qualifyAsStringWith(nexusVocBase)   -> Json.fromBoolean(deprecated)
    )
  }

  "A DomainElasticIndexer" should {
    val indexer     = DomainElasticIndexer(client, settings)
    val id          = DomainId(OrgId("org"), "dom")
    val description = "description"
    val meta        = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())

    "index a DomainCreated event" in {
      val indexId = id.toIndex(indexPrefix)

      whenReady(client.existsIndex(indexId).failed) { e =>
        e shouldBe a[ElasticClientError]
      }
      val rev = 1L

      indexer(DomainCreated(id, rev, meta, description)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        rs.results.head.source shouldEqual expectedJson(id, rev, description, false, meta, meta)
        client.existsIndex(indexId).futureValue shouldEqual (())
      }
    }

    "index a DomainDeprecated event" in {
      val metaUpdated = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())

      val rev = 2L
      indexer(DomainDeprecated(id, rev, metaUpdated)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        rs.results.head.source shouldEqual expectedJson(id, rev, description, true, metaUpdated, meta)
      }
    }
  }
}
