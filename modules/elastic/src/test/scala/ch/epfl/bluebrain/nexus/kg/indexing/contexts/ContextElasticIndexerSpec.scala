package ch.epfl.bluebrain.nexus.kg.indexing.contexts

import java.time.Clock
import java.util.regex.Pattern

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
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexingSettings, IndexerFixture}
import io.circe.{Decoder, Json}
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._

class ContextElasticIndexerSpec
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
  private implicit val stringQualifier: ConfiguredQualifier[String]   = Qualifier.configured[String](nexusVocBase)
  implicit val orgIdQualifier: ConfiguredQualifier[OrgId]             = Qualifier.configured[OrgId](base)
  implicit val domainIdQualifier: ConfiguredQualifier[DomainId]       = Qualifier.configured[DomainId](base)
  implicit val contextNameQualifier: ConfiguredQualifier[ContextName] = Qualifier.configured[ContextName](base)

  private def getAll: Future[QueryResults[Json]] =
    client.search[Json](Json.obj("query" -> Json.obj("match_all" -> Json.obj())))(Pagination(0, 100))

  private def expectedJson(id: ContextId,
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
      "published".qualifyAsStringWith(nexusVocBase)    -> Json.fromBoolean(published),
      updatedAtTimeKey                                 -> Json.fromString(meta.instant.toString),
      rdfTypeKey                                       -> Json.fromString("Context".qualifyAsString),
      contextGroupKey                                  -> Json.fromString(id.contextName.qualifyAsString),
      "deprecated".qualifyAsStringWith(nexusVocBase)   -> Json.fromBoolean(deprecated)
    )
  }

  "A ContextElasticIndexer" should {
    val indexer      = ContextElasticIndexer(client, settings)
    val orgId        = OrgId(genId())
    val domainId     = DomainId(orgId, genId())
    val id           = ContextId(domainId, genName(), genVersion())
    val meta         = Meta(Anonymous(), Clock.systemUTC.instant())
    val replacements = Map(Pattern.quote("{{base}}") -> base.toString, Pattern.quote("{{context}}") -> id.show)

    "index a ContextCreated event" in {
      val indexId = id.toIndex(indexPrefix)

      whenReady(client.existsIndex(indexId).failed) { e =>
        e shouldBe a[ElasticClientError]
      }
      val rev  = 1L
      val data = jsonContentOf("/contexts/minimal.json", replacements)
      indexer(ContextCreated(id, rev, meta, data)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        rs.results.head.source shouldEqual data.deepMerge(
          expectedJson(id, rev, deprecated = false, published = false, meta, meta))
        client.existsIndex(indexId).futureValue shouldEqual (())
      }
    }

    val data =
      jsonContentOf("/contexts/minimal_updated.json", replacements + (Pattern.quote("{{random}}") -> genString()))
    "index a ContextUpdated event" in {
      val metaUpdated = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev         = 2L
      indexer(ContextUpdated(id, rev, metaUpdated, data)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        rs.results.head.source shouldEqual data.deepMerge(
          expectedJson(id, rev, deprecated = false, published = false, metaUpdated, meta))
      }
    }

    val metaPublished = Meta(Anonymous(), Clock.systemUTC.instant())
    "index a ContextPublished event" in {
      val rev = 3L
      indexer(ContextPublished(id, rev, metaPublished)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        rs.results.head.source shouldEqual data
          .deepMerge(expectedJson(id, rev, deprecated = false, published = true, metaPublished, meta))
          .deepMerge(Json.obj(publishedAtTimeKey -> Json.fromString(metaPublished.instant.toString)))
      }
    }

    "index a ContextDeprecated event" in {
      val metaUpdated = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev         = 4L
      indexer(ContextDeprecated(id, rev, metaUpdated)).futureValue
      eventually {
        val rs = getAll.futureValue
        rs.results.size shouldEqual 1
        rs.results.head.source shouldEqual data
          .deepMerge(expectedJson(id, rev, deprecated = true, published = true, metaUpdated, meta))
          .deepMerge(Json.obj(publishedAtTimeKey -> Json.fromString(metaPublished.instant.toString)))
      }
    }
  }
}
