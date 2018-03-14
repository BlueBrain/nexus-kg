package ch.epfl.bluebrain.nexus.kg.indexing.domains

import java.time.Clock

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlClient}
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent.{DomainCreated, DomainDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.indexing.IndexerFixture
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

@DoNotDiscover
class DomainSparqlIndexerSpec(blazegraphPort: Int)
    extends TestKit(ActorSystem("DomainSparqlIndexerSpec"))
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

  private val base         = s"http://$localhost/v0"
  private val baseUri: Uri = s"http://$localhost:$blazegraphPort/blazegraph"
  private val namespace    = genString(length = 6)

  private val settings @ DomainSparqlIndexingSettings(domainsBase, _, nexusVocBase) =
    DomainSparqlIndexingSettings(base, s"$base/domains/graphs", s"$base/voc/nexus/core")

  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]   = Qualifier.configured[OrgId](base)

  private def triples(client: SparqlClient[Future]): Future[List[(String, String, String)]] =
    client.query("SELECT * { ?s ?p ?o }").map { rs =>
      rs.asScala.toList.map { qs =>
        val obj = {
          val node = qs.get("?o")
          if (node.isLiteral) node.asLiteral().getLexicalForm
          else node.asResource().toString
        }
        (qs.get(s"?$subject").toString, qs.get("?p").toString, obj)
      }
    }

  private def expectedTriples(id: DomainId,
                              rev: Long,
                              deprecated: Boolean,
                              description: String,
                              meta: Meta): List[(String, String, String)] = {
    val qualifiedId = id.qualifyAsStringWith(domainsBase)
    List(
      (qualifiedId, "rev" qualifyAsStringWith nexusVocBase, rev.toString),
      (qualifiedId, "deprecated" qualifyAsStringWith nexusVocBase, deprecated.toString),
      (qualifiedId, "description" qualifyAsStringWith nexusVocBase, description),
      (qualifiedId, "organization" qualifyAsStringWith nexusVocBase, id.orgId.qualifyAsString),
      (qualifiedId, updatedAtTimeKey, meta.instant.toString),
      (qualifiedId, rdfTypeKey, "Domain".qualifyAsString),
      (qualifiedId, "name" qualifyAsStringWith nexusVocBase, id.id)
    )

  }

  "A DomainSparqlIndexer" should {
    val client  = BlazegraphClient[Future](baseUri, namespace, None)
    val indexer = DomainSparqlIndexer(client, settings)

    val id = DomainId(OrgId("org"), "dom")

    val description = "description"
    val meta        = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())

    "index a DomainCreated event" in {
      client.createNamespace(properties).futureValue
      val rev = 1L
      indexer(DomainCreated(id, rev, meta, description)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 8
      rs should contain theSameElementsAs expectedTriples(id, rev, deprecated = false, description, meta) ++ Set(
        (id.qualifyAsStringWith(domainsBase), createdAtTimeKey, meta.instant.toString))
    }

    "index a DomainDeprecated event" in {
      val metaUpdated = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())

      val rev = 2L
      indexer(DomainDeprecated(id, rev, metaUpdated)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 8
      rs should contain theSameElementsAs expectedTriples(id, rev, deprecated = true, description, metaUpdated) ++ Set(
        (id.qualifyAsStringWith(domainsBase), createdAtTimeKey, meta.instant.toString))
    }
  }
}
