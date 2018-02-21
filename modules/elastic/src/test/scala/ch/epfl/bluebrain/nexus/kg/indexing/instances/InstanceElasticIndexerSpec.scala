package ch.epfl.bluebrain.nexus.kg.indexing.instances

import java.time.Clock
import java.util.UUID

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
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexingSettings, IndexerFixture}
import io.circe.{Decoder, Json}
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._

class InstanceElasticIndexerSpec
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

  private val settings @ ElasticIndexingSettings(indexPrefix, _, instanceBase, nexusVocBase) =
    ElasticIndexingSettings(genString(length = 6), genString(length = 6), base, s"$base/voc/nexus/core")
  private implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  private implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)
  private implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId]     = Qualifier.configured[SchemaId](base)

  private def getAll: Future[QueryResults[Json]] =
    client.search[Json](Json.obj("query" -> Json.obj("match_all" -> Json.obj())))(Pagination(0, 100))

  private def expectedJson(id: InstanceId, rev: Long, deprecated: Boolean, meta: Meta, firstReqMeta: Meta): Json = {
    Json.obj(
      createdAtTimeKey                                 -> Json.fromString(firstReqMeta.instant.toString),
      idKey                                            -> Json.fromString(id.qualifyAsStringWith(instanceBase)),
      updatedAtTimeKey                                 -> Json.fromString(meta.instant.toString),
      "rev".qualifyAsStringWith(nexusVocBase)          -> Json.fromLong(rev),
      "deprecated".qualifyAsStringWith(nexusVocBase)   -> Json.fromBoolean(deprecated),
      "organization".qualifyAsStringWith(nexusVocBase) -> Json.fromString(id.schemaId.domainId.orgId.qualifyAsString),
      "domain".qualifyAsStringWith(nexusVocBase)       -> Json.fromString(id.schemaId.domainId.qualifyAsString),
      "schema".qualifyAsStringWith(nexusVocBase)       -> Json.fromString(id.schemaId.qualifyAsString),
      schemaGroupKey                                   -> Json.fromString(id.schemaId.schemaName.qualifyAsString),
      "uuid".qualifyAsStringWith(nexusVocBase)         -> Json.fromString(id.id),
      rdfTypeKey                                       -> Json.fromString("Instance".qualifyAsString)
    )
  }

  private def expectedJsonAttachment(id: InstanceId,
                                     rev: Long,
                                     deprecated: Boolean,
                                     meta: Attachment.Meta,
                                     metaUser: Meta,
                                     firstReqMeta: Meta): Json = {
    val Attachment.Meta(_, Info(originalFileName, mediaType, Size(_, contentSize), Digest(algorithm, digest))) = meta
    expectedJson(id, rev, deprecated, metaUser, firstReqMeta) deepMerge Json.obj(
      "originalFileName".qualifyAsStringWith(nexusVocBase) -> Json.fromString(originalFileName),
      "mediaType".qualifyAsStringWith(nexusVocBase)        -> Json.fromString(mediaType),
      "contentSize".qualifyAsStringWith(nexusVocBase)      -> Json.fromLong(contentSize),
      "digestAlgorithm".qualifyAsStringWith(nexusVocBase)  -> Json.fromString(algorithm),
      "digest".qualifyAsStringWith(nexusVocBase)           -> Json.fromString(digest)
    )
  }

  "An InstanceElasticIndexer" should {

    val (ctxs, replacements) = createContext(base)

    val indexer = InstanceElasticIndexer(client, ctxs, settings)

    val meta = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
    val id   = InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, 0)), UUID.randomUUID().toString)

    "index an InstanceCreated event" in {
      val indexId = id.toIndex(indexPrefix)

      whenReady(client.existsIndex(indexId).failed) { e =>
        e shouldBe a[ElasticClientError]
      }

      val rev  = 1L
      val data = jsonContentOf("/instances/minimal_initial.json", replacements + ("random" -> "updated"))

      indexer(InstanceCreated(id, rev, meta, data)).futureValue

      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json.deepMerge(expectedJson(id, rev, deprecated = false, meta, meta))
        client.existsIndex(indexId).futureValue shouldEqual (())
      }
    }

    val data = jsonContentOf("/instances/minimal.json", replacements)
    "index an InstanceUpdated event" in {
      val metaUpdated = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
      val rev         = 2L
      indexer(InstanceUpdated(id, rev, metaUpdated, data)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json.deepMerge(expectedJson(id, rev, deprecated = false, metaUpdated, meta))
      }
    }

    "index an InstanceAttachmentCreated" in {
      val metaUpdated = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
      val rev         = 3L
      val attMeta =
        Attachment.Meta("uri", Info("filename", "contenttype", Size("byte", 1024L), Digest("SHA-256", "asd123")))
      indexer(InstanceAttachmentCreated(id, rev, metaUpdated, attMeta)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json.deepMerge(
          expectedJsonAttachment(id, rev, deprecated = false, attMeta, metaUpdated, meta))
      }
    }

    val attMeta =
      Attachment.Meta("uri",
                      Info("filename-update", "contenttype-updated", Size("byte", 1025L), Digest("SHA-256", "asd1234")))
    "index a subsequent InstanceAttachmentCreated" in {
      val metaUpdated = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
      val rev         = 4L

      indexer(InstanceAttachmentCreated(id, rev, metaUpdated, attMeta)).futureValue

      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(data).futureValue
        rs.results.head.source shouldEqual json.deepMerge(
          expectedJsonAttachment(id, rev, deprecated = false, attMeta, metaUpdated, meta))
      }
    }

    val dataUpdated = Json.obj("one" -> Json.fromString(genString()))
    "index another InstanceUpdated event" in {
      val metaUpdated = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
      val rev         = 5L
      indexer(InstanceUpdated(id, rev, metaUpdated, dataUpdated)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(dataUpdated).futureValue
        rs.results.head.source shouldEqual json.deepMerge(
          expectedJsonAttachment(id, rev, deprecated = false, attMeta, metaUpdated, meta))
      }
    }

    "index an InstanceAttachmentRemoved" in {
      val metaUpdated = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
      val rev         = 6L
      indexer(InstanceAttachmentRemoved(id, rev, metaUpdated)).futureValue

      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(dataUpdated).futureValue
        rs.results.head.source shouldEqual json.deepMerge(expectedJson(id, rev, deprecated = false, metaUpdated, meta))
      }
    }

    "index an InstanceDeprecated event" in {
      val metaUpdated = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
      val rev         = 7L
      indexer(InstanceDeprecated(id, rev, metaUpdated)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        val json = ctxs.resolve(dataUpdated).futureValue
        rs.results.head.source shouldEqual json.deepMerge(expectedJson(id, rev, deprecated = true, metaUpdated, meta))
      }
    }

    "index two objects with one field with two different types" in {
      val id1 =
        InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, 0)), UUID.randomUUID().toString)
      val id2 =
        InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, 0)), UUID.randomUUID().toString)

      val rev   = 1L
      val data1 = jsonContentOf("/instances/instance_string_field.json", replacements + ("random" -> "updated"))
      val data2 = jsonContentOf("/instances/instance_object_field.json", replacements + ("random" -> "updated"))

      indexer(InstanceCreated(id1, rev, meta, data1)).futureValue
      indexer(InstanceCreated(id2, rev, meta, data2)).futureValue

      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 3
      }

    }
  }
}
