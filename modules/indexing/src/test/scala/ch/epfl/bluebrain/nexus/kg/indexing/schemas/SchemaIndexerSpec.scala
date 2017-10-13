package ch.epfl.bluebrain.nexus.kg.indexing.schemas

import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent._
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.indexing.IndexerFixture
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
class SchemaIndexerSpec(blazegraphPort: Int)
    extends TestKit(ActorSystem("SchemaIndexerSpec"))
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

  private val settings @ SchemaIndexingSettings(index, schemasBase, schemasBaseNs, nexusVocBase) =
    SchemaIndexingSettings(genString(length = 6), base, s"$base/schemas/graphs", s"$base/voc/nexus/core")

  private implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  private implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)

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

  private def expectedTriples(id: SchemaId,
                              rev: Long,
                              deprecated: Boolean,
                              published: Boolean,
                              description: String): List[(String, String, String)] = {
    val qualifiedId = id.qualifyAsStringWith(schemasBase)
    List(
      (qualifiedId, "rev" qualifyAsStringWith nexusVocBase, rev.toString),
      (qualifiedId, "deprecated" qualifyAsStringWith nexusVocBase, deprecated.toString),
      (qualifiedId, "published" qualifyAsStringWith nexusVocBase, published.toString),
      (qualifiedId, "desc" qualifyAsStringWith nexusVocBase, description),
      (qualifiedId, "organization" qualifyAsStringWith nexusVocBase, id.domainId.orgId.qualifyAsString),
      (qualifiedId, "domain" qualifyAsStringWith nexusVocBase, id.domainId.qualifyAsString),
      (qualifiedId, "name" qualifyAsStringWith nexusVocBase, id.name),
      (qualifiedId, schemaGroupKey, id.schemaName.qualifyAsString),
      (qualifiedId, rdfTypeKey, "Schema".qualifyAsString),
      (qualifiedId, "version" qualifyAsStringWith nexusVocBase, id.version.show)
    )
  }

  "A SchemaIndexer" should {
    val client  = SparqlClient[Future](baseUri)
    val indexer = SchemaIndexer(client, settings)

    val id = SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, 0))

    "index a SchemaCreated event" in {
      client.createIndex(index, properties).futureValue
      val rev  = 1L
      val data = jsonContentOf("/schemas/minimal.json", replacements)
      indexer(SchemaCreated(id, rev, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 16
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, published = false, "random")
    }

    "index a SchemaUpdated event" in {
      val rev  = 2L
      val data = jsonContentOf("/schemas/minimal.json", replacements + ("random" -> "updated"))
      indexer(SchemaUpdated(id, rev, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 16
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, published = false, "updated")
    }

    "index a SchemaPublished event" in {
      val rev = 3L
      indexer(SchemaPublished(id, rev)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 16
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, published = true, "updated")
    }

    "index a SchemaDeprecated event" in {
      val rev = 4L
      indexer(SchemaDeprecated(id, rev)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 16
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = true, published = true, "updated")
    }
  }
}
