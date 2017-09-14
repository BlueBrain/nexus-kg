package ch.epfl.bluebrain.nexus.kg.indexing.domains

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent.{DomainCreated, DomainDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.kg.indexing.IndexerFixture
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

@DoNotDiscover
class DomainIndexerSpec(blazegraphPort: Int)
  extends TestKit(ActorSystem("DomainIndexerSpec"))
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

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 100 milliseconds)

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val mt: ActorMaterializer = ActorMaterializer()

  private implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient
  private implicit val rs: HttpClient[Future, ResultSet] = HttpClient.withAkkaUnmarshaller[ResultSet]

  private val base = s"http://$localhost/v0"
  private val baseUri: Uri = s"http://$localhost:$blazegraphPort/blazegraph"

  private val settings@DomainIndexingSettings(index, domainsBase, domainsBaseNs, nexusVocBase) =
    DomainIndexingSettings(
      genString(length = 6),
      base,
      s"$base/domains/graphs",
      s"$base/voc/nexus/core")

  private def triples(client: SparqlClient[Future]): Future[List[(String, String, String)]] =
    client.query(index, "SELECT * { ?s ?p ?o }").map { rs =>
      rs.asScala.toList.map { qs =>
        val obj = {
          val node = qs.get("?o")
          if (node.isLiteral) node.asLiteral().getLexicalForm
          else node.asResource().toString
        }
        (qs.get(s"?$subject").toString, qs.get("?p").toString, obj)
      }
    }

  private def expectedTriples(
    id: DomainId,
    rev: Long,
    deprecated: Boolean,
    description: String): List[(String, String, String)] = {
    val qualifiedId = id.qualifyAsStringWith(domainsBase)
    List(
      (qualifiedId, "rev"          qualifyAsStringWith nexusVocBase, rev.toString),
      (qualifiedId, "deprecated"   qualifyAsStringWith nexusVocBase, deprecated.toString),
      (qualifiedId, "description"  qualifyAsStringWith nexusVocBase, description),
      (qualifiedId, "organization" qualifyAsStringWith nexusVocBase, id.orgId.id),
      (qualifiedId, "domain"       qualifyAsStringWith nexusVocBase, id.id))
  }

  "A DomainIndexer" should {
    val client = SparqlClient[Future](baseUri)
    val indexer = DomainIndexer(client, settings)

    val id = DomainId(OrgId("org"), "dom")

    val description = "description"
    "index a DomainCreated event" in {
      client.createIndex(index, properties).futureValue
      val rev = 1L
      indexer(DomainCreated(id, rev, description)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 5
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, description)
    }

    "index a DomainDeprecated event" in {
      val rev = 2L
      indexer(DomainDeprecated(id, rev)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 5
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = true, description)
    }
  }
}