package ch.epfl.bluebrain.nexus.kg.indexing.schemas

import java.time.Clock

import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder, ElasticQueryClient}
import ch.epfl.bluebrain.nexus.commons.es.server.embed.ElasticServer
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexingSettings, IndexerFixture}
import io.circe.{Decoder, Json}
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._

class SchemaElasticIndexerSpec
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
  private implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  private implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)

  private def getAll: Future[QueryResults[Json]] =
    client.search[Json](Json.obj("query" -> Json.obj("match_all" -> Json.obj())))(Pagination(0, 100))

  private def expectedJson(id: SchemaId,
                           rev: Long,
                           deprecated: Boolean,
                           published: Boolean,
                           meta: Meta,
                           firstReqMeta: Meta): Json = {
    Json.obj(
      createdAtTimeKey                                 -> Json.fromString(firstReqMeta.instant.toString),
      idKey                                            -> Json.fromString(id.qualifyAsStringWith(orgBase)),
      "rev".qualifyAsStringWith(nexusVocBase)          -> Json.fromLong(rev),
      "organization".qualifyAsStringWith(nexusVocBase) -> Json.fromString(id.domainId.orgId.qualifyAsString),
      "domain".qualifyAsStringWith(nexusVocBase)       -> Json.fromString(id.domainId.qualifyAsString),
      "name".qualifyAsStringWith(nexusVocBase)         -> Json.fromString(id.name),
      "version".qualifyAsStringWith(nexusVocBase)      -> Json.fromString(id.version.show),
      schemaGroupKey                                   -> Json.fromString(id.schemaName.qualifyAsString),
      "published".qualifyAsStringWith(nexusVocBase)    -> Json.fromBoolean(published),
      updatedAtTimeKey                                 -> Json.fromString(meta.instant.toString),
      rdfTypeKey                                       -> Json.fromString("Schema".qualifyAsString),
      "deprecated".qualifyAsStringWith(nexusVocBase)   -> Json.fromBoolean(deprecated)
    )
  }

  "A SchemaElasticIndexer" should {

    val (ctxs, replacements) = createContext(base)
    val indexer              = SchemaElasticIndexer(client, ctxs, settings)

    val id   = SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, 0))
    val meta = Meta(Anonymous(), Clock.systemUTC.instant())

    "index a SchemaCreated event" in {
      val indexId = id.toIndex(indexPrefix)

      whenReady(client.existsIndex(indexId).failed) { e =>
        e shouldBe a[ElasticClientError]
      }

      val rev = 1L
      val data = jsonContentOf("/schemas/minimal.json", replacements) deepMerge Json.obj(
        genString() -> Json.fromString(genString()))
      indexer(SchemaCreated(id, rev, meta, data)).futureValue

      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json.deepMerge(
          expectedJson(id, rev, deprecated = false, published = false, meta, meta))
        client.existsIndex(indexId).futureValue shouldEqual (())
      }
    }

    val data = jsonContentOf("/schemas/minimal.json", replacements + ("random" -> "updated"))
    "index a SchemaUpdated event" in {
      val metaUpdated = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev         = 2L
      indexer(SchemaUpdated(id, rev, metaUpdated, data)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json.deepMerge(
          expectedJson(id, rev, deprecated = false, published = false, metaUpdated, meta))
      }
    }
    val metaPublished = Meta(Anonymous(), Clock.systemUTC.instant())

    "index a SchemaPublished event" in {
      val rev = 3L
      indexer(SchemaPublished(id, rev, metaPublished)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json
          .deepMerge(expectedJson(id, rev, deprecated = false, published = true, metaPublished, meta))
          .deepMerge(Json.obj(publishedAtTimeKey -> Json.fromString(metaPublished.instant.toString)))
      }
    }

    "index a SchemaDeprecated event" in {
      val metaUpdated = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev         = 4L
      indexer(SchemaDeprecated(id, rev, metaUpdated)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json
          .deepMerge(expectedJson(id, rev, deprecated = true, published = true, metaUpdated, meta))
          .deepMerge(Json.obj(publishedAtTimeKey -> Json.fromString(metaPublished.instant.toString)))
      }
    }
  }
}
