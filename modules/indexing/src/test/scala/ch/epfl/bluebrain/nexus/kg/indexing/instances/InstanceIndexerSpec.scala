package ch.epfl.bluebrain.nexus.kg.indexing.instances

import java.time.Clock
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, IndexerFixture, Qualifier}
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

@DoNotDiscover
class InstanceIndexerSpec(blazegraphPort: Int)
    extends TestKit(ActorSystem("InstanceIndexerSpec"))
    with IndexerFixture
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Randomness
    with Resources
    with Inspectors
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(3 seconds, 100 milliseconds)

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val mt: ActorMaterializer        = ActorMaterializer()

  private implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient
  private implicit val rs: HttpClient[Future, ResultSet] =
    HttpClient.withAkkaUnmarshaller[ResultSet]

  private val base                   = s"http://$localhost/v0"
  private val blazegraphBaseUri: Uri = s"http://$localhost:$blazegraphPort/blazegraph"

  private val settings @ InstanceIndexingSettings(index, instanceBase, instanceBaseNs, nexusVocBase) =
    InstanceIndexingSettings(genString(length = 6), base, s"$base/data/graphs", s"$base/voc/nexus/core")

  private implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  private implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId]     = Qualifier.configured[SchemaId](base)
  private implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)

  private def triples(id: InstanceId, client: SparqlClient[Future]): Future[List[(String, String, String)]] =
    client
      .query(index, s"SELECT * WHERE { GRAPH <${id.qualifyAsStringWith(instanceBaseNs)}> { ?s ?p ?o } }")
      .map { rs =>
        rs.asScala.toList.map { qs =>
          val obj = {
            val node = qs.get("?o")
            if (node.isLiteral) node.asLiteral().getLexicalForm
            else node.asResource().toString
          }
          (qs.get(s"?$subject").toString, qs.get("?p").toString, obj)
        }
      }

  private def allTriples(id: InstanceId, client: SparqlClient[Future]): Future[List[(String, String, String)]] =
    client.query(index, s"SELECT * WHERE { ?s ?p ?o }").map { rs =>
      rs.asScala.toList.map { qs =>
        val obj = {
          val node = qs.get("?o")
          if (node.isLiteral) node.asLiteral().getLexicalForm
          else node.asResource().toString
        }
        (qs.get(s"?$subject").toString, qs.get("?p").toString, obj)
      }
    }

  private def expectedTriples(id: InstanceId,
                              rev: Long,
                              deprecated: Boolean,
                              description: String): List[(String, String, String)] = {
    val qualifiedId = id.qualifyAsStringWith(instanceBase)
    List(
      (qualifiedId, "rev" qualifyAsStringWith nexusVocBase, rev.toString),
      (qualifiedId, "deprecated" qualifyAsStringWith nexusVocBase, deprecated.toString),
      (qualifiedId, "desc" qualifyAsStringWith nexusVocBase, description),
      (qualifiedId, "organization" qualifyAsStringWith nexusVocBase, id.schemaId.domainId.orgId.qualifyAsString),
      (qualifiedId, "domain" qualifyAsStringWith nexusVocBase, id.schemaId.domainId.qualifyAsString),
      (qualifiedId, "schema" qualifyAsStringWith nexusVocBase, id.schemaId.qualifyAsString),
      (qualifiedId, rdfTypeKey, "Instance".qualifyAsString),
      (qualifiedId, "uuid" qualifyAsStringWith nexusVocBase, id.id.show),
      (id.schemaId.qualifyAsString, schemaGroupKey, id.schemaId.schemaName.qualifyAsString),
    )
  }

  private def expectedTriples(id: InstanceId,
                              rev: Long,
                              deprecated: Boolean,
                              description: String,
                              meta: Attachment.Meta): List[(String, String, String)] = {
    val qualifiedId                                                                                       = id.qualifyAsStringWith(instanceBase)
    val Attachment.Meta(_, Info(originalFileName, contentType, Size(_, size), Digest(algorithm, digest))) = meta
    expectedTriples(id, rev, deprecated, description) ++
      List(
        (qualifiedId, "originalFileName" qualifyAsStringWith nexusVocBase, originalFileName),
        (qualifiedId, "contentType" qualifyAsStringWith nexusVocBase, contentType),
        (qualifiedId, "size" qualifyAsStringWith nexusVocBase, size.toString),
        (qualifiedId, "digestAlgorithm" qualifyAsStringWith nexusVocBase, algorithm),
        (qualifiedId, "digest" qualifyAsStringWith nexusVocBase, digest)
      )
  }

  "An InstanceIndexer" should {

    val client = SparqlClient[Future](blazegraphBaseUri)

    val (ctxs, replacements) = createContext(base)

    val indexer = InstanceIndexer(client, ctxs, settings)

    val meta = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
    val id   = InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, 0)), UUID.randomUUID().toString)

    "index an InstanceCreated event" in {
      client.createIndex(index, properties).futureValue
      val rev  = 1L
      val data = jsonContentOf("/instances/minimal.json", replacements)
      indexer(InstanceCreated(id, rev, meta, data)).futureValue
      val rs = triples(id, client).futureValue
      rs.size shouldEqual 9
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, "random")
    }

    "index an InstanceUpdated event" in {
      val rev  = 2L
      val data = jsonContentOf("/instances/minimal.json", replacements + ("random" -> "updated"))
      indexer(InstanceUpdated(id, rev, meta, data)).futureValue
      val rs = triples(id, client).futureValue
      rs.size shouldEqual 9
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, "updated")
    }

    "index an InstanceAttachmentCreated" in {
      val rev = 3L
      val attMeta =
        Attachment.Meta("uri", Info("filename", "contenttype", Size("byte", 1024L), Digest("SHA-256", "asd123")))
      indexer(InstanceAttachmentCreated(id, rev, meta, attMeta)).futureValue
      val rs = allTriples(id, client).futureValue
      rs.size shouldEqual 14
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, "updated", attMeta)
    }

    "index a subsequent InstanceAttachmentCreated" in {
      val rev = 4L
      val attMeta =
        Attachment.Meta(
          "uri",
          Info("filename-update", "contenttype-updated", Size("byte", 1025L), Digest("SHA-256", "asd1234")))
      indexer(InstanceAttachmentCreated(id, rev, meta, attMeta)).futureValue
      val rs = allTriples(id, client).futureValue
      rs.size shouldEqual 14
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, "updated", attMeta)
    }

    "index an InstanceAttachmentRemoved" in {
      val rev = 5L
      indexer(InstanceAttachmentRemoved(id, rev, meta)).futureValue
      val rs = allTriples(id, client).futureValue
      rs.size shouldEqual 9
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, "updated")
    }

    "index an InstanceDeprecated event" in {
      val rev = 6L
      indexer(InstanceDeprecated(id, rev, meta)).futureValue
      val rs = triples(id, client).futureValue
      rs.size shouldEqual 9
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = true, "updated")
    }
  }
}
