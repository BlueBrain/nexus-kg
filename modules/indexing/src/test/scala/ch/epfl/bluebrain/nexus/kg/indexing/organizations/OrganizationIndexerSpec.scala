package ch.epfl.bluebrain.nexus.kg.indexing.organizations

import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.IndexerFixture
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

@DoNotDiscover
class OrganizationIndexerSpec(blazegraphPort: Int)
    extends TestKit(ActorSystem("OrganizationIndexerSpec"))
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

  private val settings @ OrganizationIndexingSettings(index, orgBase, _, nexusVocBase) =
    OrganizationIndexingSettings(genString(length = 6), base, s"$base/organizations/graphs", s"$base/voc/nexus/core")

  private val replacements = Map(Pattern.quote("{{base}}") -> base)

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

  private def expectedTriples(id: OrgId,
                              rev: Long,
                              deprecated: Boolean,
                              description: String): List[(String, String, String)] = {

    val qualifiedId = id.qualifyAsStringWith(orgBase)
    List(
      (qualifiedId, "rev" qualifyAsStringWith nexusVocBase, rev.toString),
      (qualifiedId, "deprecated" qualifyAsStringWith nexusVocBase, deprecated.toString),
      (qualifiedId, "desc" qualifyAsStringWith nexusVocBase, description),
      (qualifiedId, rdfTypeKey, "Organization" qualifyAsStringWith nexusVocBase),
      (qualifiedId, "name" qualifyAsStringWith nexusVocBase, id.id)
    )
  }

  "A OrganizationIndexer" should {
    val client  = SparqlClient[Future](baseUri)
    val indexer = OrganizationIndexer(client, settings)

    val id = OrgId(genString(length = 4))

    "index a OrgCreated event" in {
      client.createIndex(index, properties).futureValue
      val rev  = 1L
      val data = jsonContentOf("/instances/minimal.json", replacements)
      indexer(OrgCreated(id, rev, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 5
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, "random")
    }

    "index a OrgUpdated event" in {
      val rev  = 2L
      val data = jsonContentOf("/instances/minimal.json", replacements + ("random" -> "updated"))
      indexer(OrgUpdated(id, rev, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 5
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, "updated")
    }

    "index a OrgDeprecated event" in {
      val rev = 3L
      indexer(OrgDeprecated(id, rev)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 5
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = true, "updated")
    }
  }
}
