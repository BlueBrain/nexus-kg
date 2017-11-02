package ch.epfl.bluebrain.nexus.kg.indexing.schemas

import java.time.Clock
import java.util.regex.Pattern

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
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, IndexerFixture, Qualifier}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
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
    PatienceConfig(5 seconds, 100 milliseconds)

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val mt: ActorMaterializer        = ActorMaterializer()

  private implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient
  private implicit val rs: HttpClient[Future, ResultSet] =
    HttpClient.withAkkaUnmarshaller[ResultSet]
  private implicit val clock  = Clock.systemUTC
  private implicit val caller = AnonymousCaller

  private val base                   = s"http://$localhost/v0"
  private val blazegraphBaseUri: Uri = s"http://$localhost:$blazegraphPort/blazegraph"

  private val settings @ SchemaIndexingSettings(index, schemasBase, schemasBaseNs, nexusVocBase) =
    SchemaIndexingSettings(genString(length = 6), base, s"$base/schemas/graphs", s"$base/voc/nexus/core")

  private implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  private implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)

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

    val orgsAgg = MemoryAggregate("org")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]

    val domAgg =
      MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval)
        .toF[Future]
    val ctxsAgg =
      MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval)
        .toF[Future]
    val orgs = Organizations(orgsAgg)

    val doms   = Domains(domAgg, orgs)
    val ctxs   = Contexts(ctxsAgg, doms, base.toString)
    val client = SparqlClient[Future](blazegraphBaseUri)

    val indexer = SchemaIndexer(client, ctxs, settings)
    val orgRef  = orgs.create(OrgId(genId()), genJson()).futureValue

    val domRef =
      doms.create(DomainId(orgRef.id, genId()), "domain").futureValue
    val contextId = ContextId(domRef.id, genName(), genVersion())

    val replacements = Map(Pattern.quote("{{base}}") -> base, Pattern.quote("{{context}}") -> contextId.show)

    val contextJson = jsonContentOf("/contexts/minimal.json", replacements)

    ctxs.create(contextId, contextJson).futureValue
    ctxs.publish(contextId, 1L).futureValue

    val id   = SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, 0))
    val meta = Meta(Anonymous, Clock.systemUTC.instant())

    "index a SchemaCreated event" in {
      client.createIndex(index, properties).futureValue
      val rev  = 1L
      val data = jsonContentOf("/schemas/minimal.json", replacements)
      indexer(SchemaCreated(id, rev, meta, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 16
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, published = false, "random")
    }

    "index a SchemaUpdated event" in {
      val rev  = 2L
      val data = jsonContentOf("/schemas/minimal.json", replacements + ("random" -> "updated"))
      indexer(SchemaUpdated(id, rev, meta, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 16
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, published = false, "updated")
    }

    "index a SchemaPublished event" in {
      val rev = 3L
      indexer(SchemaPublished(id, rev, meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 16
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = false, published = true, "updated")
    }

    "index a SchemaDeprecated event" in {
      val rev = 4L
      indexer(SchemaDeprecated(id, rev, meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 16
      rs should contain allElementsOf expectedTriples(id, rev, deprecated = true, published = true, "updated")
    }
  }
}
