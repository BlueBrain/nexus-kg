package ch.epfl.bluebrain.nexus.kg.indexing.query

import java.time.Clock
import java.util.UUID
import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent.{DomainCreated, DomainDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent.{OrgCreated, OrgDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent.{SchemaCreated, SchemaDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.domains.{DomainIndexer, DomainIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Filter
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op.Eq
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.LiteralTerm
import ch.epfl.bluebrain.nexus.kg.indexing.instances.{InstanceIndexer, InstanceIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.organizations.{OrganizationIndexer, OrganizationIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResult.ScoredQueryResult
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryResults.ScoredQueryResults
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.FilterQueries._
import ch.epfl.bluebrain.nexus.kg.indexing.schemas.{SchemaIndexer, SchemaIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, IndexerFixture, Qualifier}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

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

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(3 seconds, 100 milliseconds)

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val mt: ActorMaterializer        = ActorMaterializer()

  private implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient
  private implicit val rs: HttpClient[Future, ResultSet] =
    HttpClient.withAkkaUnmarshaller[ResultSet]

  private val base         = s"http://$localhost/v0"
  private val baseUri: Uri = s"http://$localhost:$blazegraphPort/blazegraph"

  private val settings @ InstanceIndexingSettings(index, instanceBase, instanceBaseNs, nexusVocBase) =
    InstanceIndexingSettings(genString(length = 6), base, s"$base/data/graphs", s"$base/voc/nexus/core")

  private val settingsSchemas @ SchemaIndexingSettings(indexSchemas, schemaBase, schemaBaseNs, nexusVocBaseSchemas) =
    SchemaIndexingSettings(genString(length = 6), base, s"$base/schemas/graphs", s"$base/voc/nexus/core")

  private val settingsDomains @ DomainIndexingSettings(indexDomains, domainBase, domainBaseNs, nexusVocBaseDomains) =
    DomainIndexingSettings(genString(length = 6), base, s"$base/domains/graphs", s"$base/voc/nexus/core")

  private val settingsOrgs @ OrganizationIndexingSettings(indexOrgs, orgBase, orgBaseNs, nexusVocBaseOrgs) =
    OrganizationIndexingSettings(genString(length = 6), base, s"$base/organizations/graphs", s"$base/voc/nexus/core")

  private val replacements = Map(Pattern.quote("{{base}}") -> base)
  private implicit val instanceQualifier: ConfiguredQualifier[InstanceId] =
    Qualifier.configured[InstanceId](base)
  private implicit val schemasQualifier: ConfiguredQualifier[SchemaId] =
    Qualifier.configured[SchemaId](base)
  private implicit val domainsQualifier: ConfiguredQualifier[DomainId] =
    Qualifier.configured[DomainId](base)
  private implicit val orgsQualifier: ConfiguredQualifier[OrgId] =
    Qualifier.configured[OrgId](base)
  private implicit val StringQualifier: ConfiguredQualifier[String] =
    Qualifier.configured[String](nexusVocBaseDomains)

  "A SparqlQuery" should {
    val orgsAgg = MemoryAggregate("org")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]

    val domAgg =
      MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval)
        .toF[Future]
    val ctxsAgg =
      MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval)
        .toF[Future]
    val orgs    = Organizations(orgsAgg)

    val doms    = Domains(domAgg, orgs)
    val ctxs    = Contexts(ctxsAgg, doms, baseUri.toString())

    val client          = SparqlClient[Future](baseUri)
    val queryClient     = new SparqlQuery[Future](client)
    val instanceIndexer = InstanceIndexer(client, settings)
    val schemaIndexer   = SchemaIndexer(ctxs, client, settingsSchemas)
    val domainIndexer   = DomainIndexer(client, settingsDomains)
    val orgIndexer      = OrganizationIndexer(client, settingsOrgs)

    val rev                  = 1L
    val data                 = jsonContentOf("/instances/minimal.json", replacements)
    val unmatched            = jsonContentOf("/instances/minimal.json", replacements + ("random" -> "different"))
    val filterNoDepr: Filter = deprecated(Some(false), nexusVocBase)
    val meta                 = Meta(Anonymous, Clock.systemUTC.instant())

    "perform a data full text search" in {
      client.createIndex(index, properties).futureValue

      // index 5 matching instances
      (0 until 5).foreach { idx =>
        val id =
          InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, data)).futureValue
      }

      // index 5 not matching instances
      (5 until 10).foreach { idx =>
        val id =
          InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, unmatched)).futureValue
      }

      // run the query
      val pagination    = Pagination(0L, 100)
      val querySettings = QuerySettings(pagination, index, nexusVocBase, base)
      val q             = FilterQueries[Future, InstanceId](queryClient, querySettings)

      val result = q.list(filterNoDepr, pagination, Some("random")).futureValue
      result
        .asInstanceOf[ScoredQueryResults[ScoredQueryResult[String]]]
        .maxScore shouldEqual 1F
      result.total shouldEqual 5L
      result.results.size shouldEqual 5

      val pagination2 = Pagination(100L, 100)
      val result2 =
        q.list(filterNoDepr, pagination2, Some("random")).futureValue
      result2
        .asInstanceOf[ScoredQueryResults[ScoredQueryResult[String]]]
        .maxScore shouldEqual 1F
      result2.total shouldEqual 5L
      result2.results.size shouldEqual 0
    }

    "perform organizations listing search" in {
      client.createIndex(indexOrgs, properties).futureValue

      // index 5 matching organizations
      (0 until 5).foreach { idx =>
        val id = OrgId(s"org-$idx")
        orgIndexer(OrgCreated(id, rev, meta, data)).futureValue
      }

      // index 5 not matching organizations
      (5 until 10).foreach { idx =>
        val id = OrgId(s"org-$idx")
        orgIndexer(OrgCreated(id, rev, meta, data)).futureValue
        orgIndexer(OrgDeprecated(id, rev, meta)).futureValue
      }

      val pagination    = Pagination(0L, 100)
      val querySettings = QuerySettings(pagination, indexOrgs, nexusVocBaseOrgs, base)
      val q             = FilterQueries[Future, OrgId](queryClient, querySettings)

      val result = q.list(filterNoDepr, pagination, None).futureValue
      result.total shouldEqual 5L
      result.results.size shouldEqual 5
      result.results.foreach(r => {
        r.source.id should startWith("org-")
      })

      val result2 =
        q.list(Filter(ComparisonExpr(Eq, UriPath("name" qualify), LiteralTerm(""""org-0""""))), pagination, None)
          .futureValue
      result2.total shouldEqual 1L
      result2.results.size shouldEqual 1
      result2.results.foreach(r => {
        r.source shouldEqual OrgId("org-0")
      })
    }

    "perform domains listing search" in {
      client.createIndex(indexDomains, properties).futureValue

      val description = genString()
      // index 5 matching domains
      (0 until 5).foreach { idx =>
        val id = DomainId(OrgId("org"), s"domain-$idx")
        domainIndexer(DomainCreated(id, rev, meta, description)).futureValue
      }

      // index 5 not matching domains
      (5 until 10).foreach { idx =>
        val id = DomainId(OrgId("org"), s"domain-$idx")
        domainIndexer(DomainCreated(id, rev, meta, description)).futureValue
        domainIndexer(DomainDeprecated(id, rev, meta)).futureValue
      }

      // index other 5 not matching domains
      (10 until 15).foreach { idx =>
        val id = DomainId(OrgId(s"org-$idx"), s"domain-$idx")
        domainIndexer(DomainCreated(id, rev, meta, description)).futureValue
      }
      // run the query
      val pagination = Pagination(0L, 100)
      val querySettings =
        QuerySettings(pagination, indexDomains, nexusVocBaseDomains, base)
      val q = FilterQueries[Future, DomainId](queryClient, querySettings)

      val result =
        q.list(OrgId("org"), filterNoDepr, pagination, None).futureValue
      result.total shouldEqual 5L
      result.results.size shouldEqual 5
      result.results.foreach(r => {
        r.source.id should startWith("domain-")
        r.source.orgId shouldEqual OrgId("org")
      })
    }

    "perform schemas listing search" in {
      client.createIndex(indexSchemas, properties).futureValue

      // index 5 matching schemas
      (0 until 5).foreach { idx =>
        val id = SchemaId(DomainId(OrgId("org"), "dom"), genString(), Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, meta, data)).futureValue
      }

      // index 5 not matching schemas
      (5 until 10).foreach { idx =>
        val id = SchemaId(DomainId(OrgId("org"), "dom"), genString(), Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, meta, data)).futureValue
        schemaIndexer(SchemaDeprecated(id, rev, meta)).futureValue
      }

      // index other 5 not matching schemas
      (10 until 15).foreach { idx =>
        val id =
          SchemaId(DomainId(OrgId("org"), "core"), "name", Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, meta, unmatched)).futureValue
      }

      val pagination = Pagination(0L, 100)
      val querySettings =
        QuerySettings(pagination, indexSchemas, nexusVocBaseSchemas, base)
      val q = FilterQueries[Future, SchemaId](queryClient, querySettings)

      val result = q
        .list(DomainId(OrgId("org"), "dom"), filterNoDepr, pagination, None)
        .futureValue

      result.total shouldEqual 5L
      result.results.size shouldEqual 5

      result.results.foreach(r => {
        r.source.version.patch.toDouble shouldEqual 2.5 +- 2.5
        r.source.domainId shouldEqual DomainId(OrgId("org"), "dom")
      })
    }

    "perform instances listing search" in {
      val domainId   = DomainId(OrgId("test"), "dom")
      val schemaName = SchemaName(domainId, genString())
      // index 5 matching instances
      (0 until 5).foreach { idx =>
        val id = InstanceId(schemaName.versioned(Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, data)).futureValue
      }

      // index 5 not matching instances
      (5 until 10).foreach { idx =>
        val id = InstanceId(schemaName.versioned(Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, data)).futureValue
        instanceIndexer(InstanceDeprecated(id, rev + 1, meta)).futureValue
      }

      // index other 5 not matching instances
      (10 until 15).foreach { idx =>
        val id = InstanceId(SchemaId(DomainId(OrgId("other"), "dom"), schemaName.name, Version(1, 0, idx)),
                            UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, unmatched)).futureValue
      }
      // index other 5 not matching instances
      (15 until 20).foreach { idx =>
        val id = InstanceId(SchemaId(domainId, genString(), Version(1, 0, idx)), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, data)).futureValue
      }

      val pagination    = Pagination(3L, 3)
      val querySettings = QuerySettings(pagination, index, nexusVocBase, base)
      val q             = FilterQueries[Future, InstanceId](queryClient, querySettings)

      val result = q.list(schemaName, filterNoDepr, pagination, None).futureValue

      result.total shouldEqual 5L
      result.results.size shouldEqual 2
      result.results.foreach(r => {
        r.source.schemaId.version.patch.toDouble shouldEqual 3.5 +- 0.5
        r.source.schemaId.name shouldEqual schemaName.name
        r.source.schemaId.domainId shouldEqual domainId
      })

      val pagination2 = Pagination(10L, 8)
      val result2     = q.list(schemaName, filterNoDepr, pagination2, None).futureValue
      result2.total shouldEqual 5L
      result2.results.size shouldEqual 0
    }
  }
}
