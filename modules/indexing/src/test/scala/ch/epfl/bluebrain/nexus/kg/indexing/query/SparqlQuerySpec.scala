package ch.epfl.bluebrain.nexus.kg.indexing.query

import java.util.UUID
import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent.{DomainCreated, DomainDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent.{OrgCreated, OrgDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent.{SchemaCreated, SchemaDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.core.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.domains.{DomainIndexer, DomainIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.instances.{InstanceIndexer, InstanceIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.organizations.{OrganizationIndexer, OrganizationIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.ScoredQueryResult
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults.ScoredQueryResults
import ch.epfl.bluebrain.nexus.kg.indexing.schemas.{SchemaIndexer, SchemaIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, IndexerFixture, Qualifier}
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import ch.epfl.bluebrain.nexus.kg.indexing.query.IndexingVocab.SelectTerms._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

@DoNotDiscover
class SparqlQuerySpec(blazegraphPort: Int)
  extends TestKit(ActorSystem("SparqlQuerySpec"))
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

  private val settings@InstanceIndexingSettings(index, instanceBase, instanceBaseNs, nexusVocBase) =
    InstanceIndexingSettings(
      genString(length = 6),
      base,
      s"$base/data/graphs",
      s"$base/voc/nexus/core")

  private val settingsSchemas@SchemaIndexingSettings(indexSchemas, schemaBase, schemaBaseNs, nexusVocBaseSchemas) =
    SchemaIndexingSettings(
      genString(length = 6),
      base,
      s"$base/schemas/graphs",
      s"$base/voc/nexus/core")

  private val settingsDomains@DomainIndexingSettings(indexDomains, domainBase, domainBaseNs, nexusVocBaseDomains) =
    DomainIndexingSettings(
      genString(length = 6),
      base,
      s"$base/domains/graphs",
      s"$base/voc/nexus/core")

  private val settingsOrgs@OrganizationIndexingSettings(indexOrgs, orgBase, orgBaseNs, nexusVocBaseOrgs) =
    OrganizationIndexingSettings(
      genString(length = 6),
      base,
      s"$base/organizations/graphs",
      s"$base/voc/nexus/core")

  private val replacements = Map(Pattern.quote("{{base}}") -> base)
  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](nexusVocBaseSchemas)
  private implicit val instanceQualifier: ConfiguredQualifier[InstanceId] = Qualifier.configured[InstanceId](base)
  private implicit val schemasQualifier: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)
  private implicit val domainsQualifier: ConfiguredQualifier[DomainId] = Qualifier.configured[DomainId](base)
  private implicit val orgsQualifier: ConfiguredQualifier[OrgId] = Qualifier.configured[OrgId](base)


  "A SparqlQuery" should {
    val client = SparqlClient[Future](baseUri)
    val instanceIndexer = InstanceIndexer(client, settings)
    val schemaIndexer = SchemaIndexer(client, settingsSchemas)
    val domainIndexer = DomainIndexer(client, settingsDomains)
    val orgIndexer = OrganizationIndexer(client, settingsOrgs)
    val query = SparqlQuery[Future](client)

    val rev = 1L
    val data = jsonContentOf("/instances/minimal.json", replacements)
    val unmatched = jsonContentOf("/instances/minimal.json", replacements + ("random" -> "different"))

    "perform a data full text search" in {
      client.createIndex(index, properties).futureValue

      // index 20 matching instances
      (0 until 20).foreach { idx =>
        val id = InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, data)).futureValue
      }

      // index 10 not matching instances
      (20 until 30).foreach { idx =>
        val id = InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, unmatched)).futureValue
      }

      // run the query
      val pagination = Pagination(0L, 100)
      val result = query.apply[InstanceId](index, FullTextSearchQuery("random", pagination).build()).futureValue
      result.asInstanceOf[ScoredQueryResults[ScoredQueryResult[String]]].maxScore shouldEqual 1F
      result.total shouldEqual 0L
      result.results.size shouldEqual 20
    }


    "perform organizations listing search" in {
      client.createIndex(indexOrgs, properties).futureValue

      // index 10 matching organizations
      (0 until 10).foreach { idx =>
        val id = OrgId(s"org-$idx")
        orgIndexer(OrgCreated(id, rev, data)).futureValue
      }

      // index 5 not matching organizations
      (10 until 15).foreach { idx =>
        val id = OrgId(s"org-$idx")
        orgIndexer(OrgCreated(id, rev, data)).futureValue
        orgIndexer(OrgDeprecated(id, rev)).futureValue
      }

      // run the query
      val pagination = Pagination(0L, 100)
      val q = QueryBuilder.select(subject)
        .where("deprecated".qualify -> "deprecated")
        .filter(s"""?deprecated = false""")
        .pagination(pagination)
        .total(subject -> total)
        .build()
      val result = query.apply[OrgId](indexOrgs, q).futureValue
      result.total shouldEqual 10L
      result.results.size shouldEqual 10
      result.results.foreach(r => {
        r.source.id should startWith("org-")
      })

      val q2 = QueryBuilder.select(subject)
        .where("organization".qualify -> "org")
        .where("deprecated".qualify -> "deprecated")
        .filter(s"""?org = "org-0" && ?deprecated = false""")
        .pagination(pagination)
        .total(subject -> total)
        .build()
      val result2 = query.apply[OrgId](indexOrgs, q2).futureValue
      result2.total shouldEqual 1L
      result2.results.size shouldEqual 1
      result2.results.foreach(r => {
        r.source shouldEqual OrgId("org-0")
      })
    }

    "perform domains listing search" in {
      client.createIndex(indexDomains, properties).futureValue

      val description = genString()
      // index 10 matching domains
      (0 until 10).foreach { idx =>
        val id = DomainId(OrgId("org"), s"domain-$idx")
        domainIndexer(DomainCreated(id, rev, description)).futureValue
      }

      // index 5 not matching domains
      (10 until 15).foreach { idx =>
        val id = DomainId(OrgId("org"), s"domain-$idx")
        domainIndexer(DomainCreated(id, rev, description)).futureValue
        domainIndexer(DomainDeprecated(id, rev)).futureValue
      }

      // index other 5 not matching domains
      (15 until 20).foreach { idx =>
        val id = DomainId(OrgId(s"org-$idx"), s"domain-$idx")
        domainIndexer(DomainCreated(id, rev, description)).futureValue
      }
      // run the query
      val pagination = Pagination(0L, 100)
      val q = QueryBuilder.select(subject)
        .where("organization".qualify -> "org")
        .where("deprecated".qualify -> "deprecated")
        .filter(s"""?org = "org" && ?deprecated = false""")
        .pagination(pagination)
        .total(subject -> total)
        .build()
      val result = query.apply[DomainId](indexDomains, q).futureValue
      result.total shouldEqual 10L
      result.results.size shouldEqual 10
      result.results.foreach(r => {
        r.source.id should startWith("domain-")
        r.source.orgId shouldEqual OrgId("org")
      })
    }

    "perform schemas listing search" in {
      client.createIndex(indexSchemas, properties).futureValue

      // index 10 matching schemas
      (0 until 10).foreach { idx =>
        val id = SchemaId(DomainId(OrgId("org"), "dom"), genString(), Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, data)).futureValue
      }

      // index 5 not matching schemas
      (10 until 15).foreach { idx =>
        val id = SchemaId(DomainId(OrgId("org"), "dom"), genString(), Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, data)).futureValue
        schemaIndexer(SchemaDeprecated(id, rev)).futureValue
      }

      // index other 5 not matching schemas
      (15 until 20).foreach { idx =>
        val id = SchemaId(DomainId(OrgId("org"), "core"), "name", Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, unmatched)).futureValue
      }
      // run the query
      val pagination = Pagination(0L, 100)
      val q = QueryBuilder.select(subject)
        .where("organization".qualify -> "org")
        .where("domain".qualify -> "domain")
        .where("deprecated".qualify -> "deprecated")
        .filter(s"""?org = "org" && ?domain = "dom" && ?deprecated = false""")
        .pagination(pagination)
        .total(subject -> total)
        .build()
      val result = query.apply[SchemaId](indexSchemas, q).futureValue
      result.total shouldEqual 10L
      result.results.size shouldEqual 10

      result.results.foreach(r => {
        r.source.version.patch shouldEqual 5 +- 5
        r.source.domainId shouldEqual DomainId(OrgId("org"), "dom")
      })
    }

    "perform instances listing search" in {
      val name = genString()
      val domainId = DomainId(OrgId("test"), "dom")
      // index 10 matching instances
      (0 until 10).foreach { idx =>
        val id = InstanceId(SchemaId(domainId, name, Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, data)).futureValue
      }

      // index 5 not matching instances
      (10 until 15).foreach { idx =>
        val id = InstanceId(SchemaId(domainId, name, Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, data)).futureValue
        instanceIndexer(InstanceDeprecated(id, rev + 1)).futureValue
      }

      // index other 5 not matching instances
      (15 until 20).foreach { idx =>
        val id = InstanceId(SchemaId(DomainId(OrgId("other"), "dom"), name, Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, unmatched)).futureValue
      }
      // index other 5 not matching instances
      (20 until 25).foreach { idx =>
        val id = InstanceId(SchemaId(domainId, genString(), Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, data)).futureValue
      }
      // run the query
      val pagination = Pagination(8L, 8)
      val q = QueryBuilder.select(subject)
        .where("organization".qualify -> "org")
        .where("domain".qualify -> "domain")
        .where("deprecated".qualify -> "deprecated")
        .where("schema".qualify -> "name")
        .filter(s"""?org = "test" && ?domain = "dom" && ?name = "$name" && ?deprecated = false""")
        .pagination(pagination)
        .total(subject -> total)
        .build()
      val result = query.apply[InstanceId](index, q).futureValue
      result.total shouldEqual 10L
      result.results.size shouldEqual 2
      result.results.foreach(r => {
        r.source.schemaId.version.patch.toDouble shouldEqual 8.5 +- 0.5
        r.source.schemaId.name shouldEqual name
        r.source.schemaId.domainId shouldEqual domainId
      })


      val pagination2 = Pagination(10L, 8)
      val q2 = QueryBuilder.select(subject)
        .where("organization".qualify -> "org")
        .where("domain".qualify -> "domain")
        .where("deprecated".qualify -> "deprecated")
        .where("schema".qualify -> "name")
        .filter(s"""?org = "test" && ?domain = "dom" && ?name = "$name" && ?deprecated = false""")
        .pagination(pagination2)
        .total(subject -> total)
        .build()
      val result2 = query.apply[InstanceId](index, q2).futureValue
      result2.total shouldEqual 10L
      result2.results.size shouldEqual 0
    }
  }
}