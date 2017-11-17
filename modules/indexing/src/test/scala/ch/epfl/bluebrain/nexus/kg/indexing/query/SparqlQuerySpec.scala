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
import ch.epfl.bluebrain.nexus.commons.iam.acls.{Meta, Permissions}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.{GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, Contexts}
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
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Event.{PermissionsAdded, PermissionsCleared}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.Read
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AuthenticatedCaller
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.indexing.acls.{AclIndexer, AclIndexingSettings}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.identity.{Identity, IdentityId}

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

  private val base                   = s"http://$localhost/v0"
  private val blazegraphBaseUri: Uri = s"http://$localhost:$blazegraphPort/blazegraph"

  val namespace = genString(length = 6)

  private val settings @ InstanceIndexingSettings(_, instanceBase, instanceBaseNs, nexusVocBase) =
    InstanceIndexingSettings(namespace, base, s"$base/data/graphs", s"$base/voc/nexus/core")

  private val settingsSchemas @ SchemaIndexingSettings(_, schemaBase, schemaBaseNs, nexusVocBaseSchemas) =
    SchemaIndexingSettings(namespace, base, s"$base/schemas/graphs", s"$base/voc/nexus/core")

  private val settingsDomains @ DomainIndexingSettings(_, domainBase, domainBaseNs, nexusVocBaseDomains) =
    DomainIndexingSettings(namespace, base, s"$base/domains/graphs", s"$base/voc/nexus/core")

  private val settingsOrgs @ OrganizationIndexingSettings(_, orgBase, orgBaseNs, nexusVocBaseOrgs) =
    OrganizationIndexingSettings(namespace, base, s"$base/organizations/graphs", s"$base/voc/nexus/core")

  private val settingsAcls @ AclIndexingSettings(_, aclBase, aclBaseNs, nexusVocBaseAcls) =
    AclIndexingSettings(namespace, base, s"$base/organizations/graphs", s"$base/voc/nexus/core")

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
  private implicit val clock = Clock.systemUTC
  private val group          = GroupRef(IdentityId(s"$base/realms/BBP/groups/group"))
  private implicit val caller =
    AuthenticatedCaller(None, Set[Identity](UserRef(IdentityId(s"$base/realms/BBP/users/1234")), group))

  "A SparqlQuery" should {
    val orgsAgg = MemoryAggregate("org")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]

    val domAgg =
      MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval)
        .toF[Future]
    val ctxsAgg =
      MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval)
        .toF[Future]
    val orgs = Organizations(orgsAgg)

    val doms = Domains(domAgg, orgs)
    val ctxs = Contexts(ctxsAgg, doms, base.toString())

    val client          = SparqlClient[Future](blazegraphBaseUri)
    val queryClient     = new SparqlQuery[Future](client)
    val instanceIndexer = InstanceIndexer(client, ctxs, settings)
    val schemaIndexer   = SchemaIndexer(client, ctxs, settingsSchemas)
    val domainIndexer   = DomainIndexer(client, settingsDomains)
    val orgIndexer      = OrganizationIndexer(client, ctxs, settingsOrgs)
    val aclIndexer      = AclIndexer(client, settingsAcls)

    val orgRef    = orgs.create(OrgId(genId()), genJson()).futureValue
    val domRef    = doms.create(DomainId(orgRef.id, genId()), "domain").futureValue
    val contextId = ContextId(domRef.id, genName(), genVersion())

    val replacements = Map(Pattern.quote("{{base}}") -> base, Pattern.quote("{{context}}") -> contextId.show)
    val contextJson  = jsonContentOf("/contexts/minimal.json", replacements)
    ctxs.create(contextId, contextJson).futureValue
    ctxs.publish(contextId, 1L).futureValue

    val rev                  = 1L
    val data                 = jsonContentOf("/instances/minimal.json", replacements)
    val unmatched            = jsonContentOf("/instances/minimal.json", replacements + ("random" -> "different"))
    val filterNoDepr: Filter = deprecated(Some(false), nexusVocBase)
    val meta                 = Meta(group, Clock.systemUTC.instant())

    "perform a data full text search" in {
      client.createIndex(namespace, properties).futureValue

      // index 5 matching instances
      (0 until 4).foreach { idx =>
        val version = Version(1, 0, idx)
        val id =
          InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), "name", version), UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, data)).futureValue
        aclIndexer(PermissionsAdded("kg" / "org" / "dom" / "name" / version.show, group, Permissions(Read), meta)).futureValue
      }

      (4 until 5).foreach { idx =>
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
      val querySettings = QuerySettings(pagination, 100, namespace, nexusVocBase, base)
      val q             = FilterQueries[Future, InstanceId](queryClient, querySettings)

      val result = q.list(filterNoDepr, pagination, Some("random")).futureValue
      result
        .asInstanceOf[ScoredQueryResults[ScoredQueryResult[String]]]
        .maxScore shouldEqual 1F
      result.total shouldEqual 4L
      result.results.size shouldEqual 4
      result.results.map(_.source.schemaId.version.patch.toDouble shouldEqual 2.0 +- 2.0)

      val pagination2 = Pagination(100L, 100)
      val result2 =
        q.list(filterNoDepr, pagination2, Some("random")).futureValue
      result2
        .asInstanceOf[ScoredQueryResults[ScoredQueryResult[String]]]
        .maxScore shouldEqual 1F
      result2.total shouldEqual 4L
      result2.results.size shouldEqual 0
    }

    "perform organizations listing search without having permissions attached" in {
      // index 5 matching organizations
      (0 until 5).foreach { idx =>
        val id = OrgId(s"org$idx")
        orgIndexer(OrgCreated(id, rev, meta, data)).futureValue
        aclIndexer(PermissionsAdded("kg" / s"org${idx}", UserRef("BBP", "1234"), Permissions(Read), meta)).futureValue
      }

      // index 5 not matching organizations
      (5 until 10).foreach { idx =>
        val id = OrgId(s"org$idx")
        orgIndexer(OrgCreated(id, rev, meta, data)).futureValue
        orgIndexer(OrgDeprecated(id, rev, meta)).futureValue
      }

      val pagination    = Pagination(0L, 100)
      val querySettings = QuerySettings(pagination, 100, namespace, nexusVocBaseOrgs, base)
      val q             = FilterQueries[Future, OrgId](queryClient, querySettings)

      val result = q.list(filterNoDepr, pagination, None).futureValue
      result.total shouldEqual 0L
      result.results.size shouldEqual 0
    }

    "perform organizations listing search" in {

      // index 3 matching organizations ACLs
      (0 until 3).foreach { idx =>
        aclIndexer(PermissionsAdded("kg" / s"org${idx}", group, Permissions(Read), meta)).futureValue
      }

      val pagination    = Pagination(0L, 100)
      val querySettings = QuerySettings(pagination, 100, namespace, nexusVocBaseOrgs, base)
      val q             = FilterQueries[Future, OrgId](queryClient, querySettings)

      val result = q.list(filterNoDepr, pagination, None).futureValue
      result.total shouldEqual 3L
      result.results.size shouldEqual 3
      result.results.foreach(r => {
        r.source.id should startWith("org")
      })

      val result2 =
        q.list(Filter(ComparisonExpr(Eq, UriPath("name" qualify), LiteralTerm(""""org0""""))), pagination, None)
          .futureValue
      result2.total shouldEqual 1L
      result2.results.size shouldEqual 1
      result2.results.foreach(r => {
        r.source shouldEqual OrgId("org0")
      })
    }

    "perform domains listing search" in {

      val description = genString()
      // index 5 matching domains and 5 other matching domains which do not have permissions
      (0 until 5).foreach { idx =>
        val id = DomainId(OrgId("org0"), s"domain$idx")
        domainIndexer(DomainCreated(id, rev, meta, description)).futureValue

        val id2 = DomainId(OrgId("org3"), s"domain$idx")
        domainIndexer(DomainCreated(id2, rev, meta, description)).futureValue
      }

      // index 5 not matching domains
      (5 until 10).foreach { idx =>
        val id = DomainId(OrgId("org0"), s"domain$idx")
        domainIndexer(DomainCreated(id, rev, meta, description)).futureValue
        domainIndexer(DomainDeprecated(id, rev, meta)).futureValue
      }

      // index other 5 not matching domains
      (10 until 15).foreach { idx =>
        val id = DomainId(OrgId(s"org$idx"), s"domain$idx")
        domainIndexer(DomainCreated(id, rev, meta, description)).futureValue
      }
      // run the query
      val pagination = Pagination(0L, 100)
      val querySettings =
        QuerySettings(pagination, 100, namespace, nexusVocBaseDomains, base)
      val q = FilterQueries[Future, DomainId](queryClient, querySettings)

      val result =
        q.list(filterNoDepr, pagination, None).futureValue
      result.total shouldEqual 5L
      result.results.size shouldEqual 5
      result.results.foreach(r => {
        r.source.id should startWith("domain")
        r.source.orgId shouldEqual OrgId("org0")
      })

      (0 until 2).foreach { idx =>
        aclIndexer(PermissionsAdded("kg" / "org3" / s"domain${idx}", group, Permissions(Read), meta)).futureValue
      }

      val result2 =
        q.list(filterNoDepr, pagination, None).futureValue
      result2.total shouldEqual 7L
      result2.results.size shouldEqual 7
      result2.results.foreach(r => {
        r.source.id should startWith("domain")
        r.source.orgId should (equal(OrgId("org0")) or equal(OrgId("org3")))
      })

    }

    "perform schemas listing search" in {

      // index 5 matching schemas
      (0 until 5).foreach { idx =>
        val id = SchemaId(DomainId(OrgId("org0"), "dom0"), genString(), Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, meta, data)).futureValue
      }

      // index 5 not matching schemas
      (5 until 10).foreach { idx =>
        val id = SchemaId(DomainId(OrgId("org0"), "dom0"), genString(), Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, meta, data)).futureValue
        schemaIndexer(SchemaDeprecated(id, rev, meta)).futureValue
      }

      // index other 5 not matching schemas
      (10 until 15).foreach { idx =>
        val id =
          SchemaId(DomainId(OrgId("org0"), "core"), "name", Version(1, 0, idx))
        schemaIndexer(SchemaCreated(id, rev, meta, unmatched)).futureValue
      }

      val pagination = Pagination(0L, 100)
      val querySettings =
        QuerySettings(pagination, 100, namespace, nexusVocBaseSchemas, base)
      val q = FilterQueries[Future, SchemaId](queryClient, querySettings)

      val result = q
        .list(DomainId(OrgId("org0"), "dom0"), filterNoDepr, pagination, None)
        .futureValue

      result.total shouldEqual 5L
      result.results.size shouldEqual 5

      result.results.foreach(r => {
        r.source.version.patch.toDouble shouldEqual 2.5 +- 2.5
        r.source.domainId shouldEqual DomainId(OrgId("org0"), "dom0")
      })

      aclIndexer(PermissionsCleared("kg" / "org0", meta)).futureValue

      val result2 = q
        .list(DomainId(OrgId("org0"), "core"), filterNoDepr, pagination, None)
        .futureValue

      result2.total shouldEqual 0L
      result2.results.size shouldEqual 0

      aclIndexer(PermissionsAdded("kg" / "org0" / "core" / "name", group, Permissions(Read), meta)).futureValue

      val result3 = q
        .list(DomainId(OrgId("org0"), "core"), filterNoDepr, pagination, None)
        .futureValue

      result3.total shouldEqual 5L
      result3.results.size shouldEqual 5

      result3.results.foreach(r => {
        r.source.version.patch.toDouble shouldEqual 12.5 +- 2.5
        r.source.domainId shouldEqual DomainId(OrgId("org0"), "core")
      })
    }

    "perform instances listing search" in {
      val domainId   = DomainId(OrgId("org0"), "dom0")
      val schemaName = SchemaName(domainId, genString())
      // index 5 matching instances
      (0 until 5).foreach { idx =>
        val schemaId = schemaName.versioned(Version(1, 0, idx))
        schemaIndexer(SchemaCreated(schemaId, rev, meta, data)).futureValue
        val id = InstanceId(schemaId, UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, data)).futureValue
      }

      // index 5 not matching instances
      (5 until 10).foreach { idx =>
        val schemaId = schemaName.versioned(Version(1, 0, idx))
        schemaIndexer(SchemaCreated(schemaId, rev, meta, data)).futureValue
        val id = InstanceId(schemaId, UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, data)).futureValue
        instanceIndexer(InstanceDeprecated(id, rev + 1, meta)).futureValue
      }

      // index other 5 not matching instances
      (10 until 15).foreach { idx =>
        val schemaId = SchemaId(DomainId(OrgId("other"), "dom"), schemaName.name, Version(1, 0, idx))
        schemaIndexer(SchemaCreated(schemaId, rev, meta, data)).futureValue
        val id = InstanceId(schemaId, UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, unmatched)).futureValue
      }
      // index other 5 not matching instances
      (15 until 20).foreach { idx =>
        val schemaId = SchemaId(domainId, genString(), Version(1, 0, idx))
        schemaIndexer(SchemaCreated(schemaId, rev, meta, data)).futureValue
        val id = InstanceId(schemaId, UUID.randomUUID().toString)
        instanceIndexer(InstanceCreated(id, rev, meta, data)).futureValue
      }

      val pagination    = Pagination(3L, 3)
      val querySettings = QuerySettings(pagination, 100, namespace, nexusVocBase, base)
      val q             = FilterQueries[Future, InstanceId](queryClient, querySettings)

      val res = q.list(schemaName, filterNoDepr, pagination, None).futureValue
      res.total shouldEqual 0L

      aclIndexer(PermissionsAdded("kg" / "org0" / "dom0" / schemaName.name, group, Permissions(Read), meta)).futureValue

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
