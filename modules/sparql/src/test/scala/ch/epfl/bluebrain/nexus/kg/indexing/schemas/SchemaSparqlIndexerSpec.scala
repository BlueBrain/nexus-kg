package ch.epfl.bluebrain.nexus.kg.indexing.schemas

import java.time.Clock

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
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlClient}
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.IndexerFixture
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

@DoNotDiscover
class SchemaSparqlIndexerSpec(blazegraphPort: Int)
    extends TestKit(ActorSystem("SchemaSparqlIndexerSpec"))
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

  private implicit val ec: ExecutionContext  = system.dispatcher
  private implicit val mt: ActorMaterializer = ActorMaterializer()

  private implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient
  private implicit val rs: HttpClient[Future, ResultSet] =
    HttpClient.withAkkaUnmarshaller[ResultSet]

  private val base                   = s"http://$localhost/v0"
  private val blazegraphBaseUri: Uri = s"http://$localhost:$blazegraphPort/blazegraph"
  private val namespace              = genString(length = 6)

  private val settings @ SchemaSparqlIndexingSettings(schemasBase, _, nexusVocBase) =
    SchemaSparqlIndexingSettings(base, s"$base/schemas/graphs", s"$base/voc/nexus/core")

  private implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  private implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)

  private val rdfSchemaBase = "http://www.w3.org/2000/01/rdf-schema#"
  private val rdfSyntaxBase = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  private val shaclBase     = "http://www.w3.org/ns/shacl#"

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

  private def expectedTriples(id: SchemaId,
                              rev: Long,
                              deprecated: Boolean,
                              published: Boolean,
                              meta: Meta,
                              firstReqMeta: Meta,
                              description: String): Set[(String, String, String)] = {
    val qualifiedId = id.qualifyAsStringWith(schemasBase)
    val shapeId     = "shapes/minimalshape".qualifyAsStringWith(base)
    Set(
      (qualifiedId, "rev" qualifyAsStringWith nexusVocBase, rev.toString),
      (qualifiedId, "deprecated" qualifyAsStringWith nexusVocBase, deprecated.toString),
      (qualifiedId, "published" qualifyAsStringWith nexusVocBase, published.toString),
      (qualifiedId, "desc" qualifyAsStringWith nexusVocBase, description),
      (qualifiedId, "organization" qualifyAsStringWith nexusVocBase, id.domainId.orgId.qualifyAsString),
      (qualifiedId, "domain" qualifyAsStringWith nexusVocBase, id.domainId.qualifyAsString),
      (qualifiedId, "name" qualifyAsStringWith nexusVocBase, id.name),
      (qualifiedId, schemaGroupKey, id.schemaName.qualifyAsString),
      (qualifiedId, createdAtTimeKey, firstReqMeta.instant.toString),
      (qualifiedId, updatedAtTimeKey, meta.instant.toString),
      (qualifiedId, rdfTypeKey, "Schema".qualifyAsString),
      (qualifiedId, "version" qualifyAsStringWith nexusVocBase, id.version.show),
      (shapeId, s"${rdfSchemaBase}isDefinedBy", qualifiedId),
      (shapeId, s"${shaclBase}targetObjectsOf", "rev" qualifyAsStringWith nexusVocBase),
      (shapeId, s"${shaclBase}description", "A resource revision number."),
      (shapeId, s"${shaclBase}datatype", "http://www.w3.org/2001/XMLSchema#integer"),
      (shapeId, s"${shaclBase}minInclusive", "1"),
      (shapeId, s"${rdfSyntaxBase}type", s"${shaclBase}NodeShape")
    )
  }

  "A SchemaSparqlIndexer" should {

    val client = BlazegraphClient[Future](blazegraphBaseUri, namespace, None)

    val (ctxs, replacements) = createContext(base)
    val indexer              = SchemaSparqlIndexer(client, ctxs, settings)

    val id   = SchemaId(DomainId(OrgId("org"), "dom"), "name", Version(1, 0, 0))
    val meta = Meta(Anonymous(), Clock.systemUTC.instant())

    "index a SchemaCreated event" in {
      client.createNamespace(properties).futureValue
      val rev  = 1L
      val data = jsonContentOf("/schemas/minimal.json", replacements)
      indexer(SchemaCreated(id, rev, meta, data)).futureValue
      val rs = triples(client).futureValue

      rs.size shouldEqual 18
      rs.toSet should contain theSameElementsAs expectedTriples(id,
                                                                rev,
                                                                deprecated = false,
                                                                published = false,
                                                                meta,
                                                                meta,
                                                                "random")
    }

    "index a SchemaUpdated event" in {
      val metaUpdated = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev         = 2L
      val data        = jsonContentOf("/schemas/minimal.json", replacements + ("random" -> "updated"))
      indexer(SchemaUpdated(id, rev, metaUpdated, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 18
      rs.toSet should contain theSameElementsAs expectedTriples(id,
                                                                rev,
                                                                deprecated = false,
                                                                published = false,
                                                                metaUpdated,
                                                                meta,
                                                                "updated")
    }
    val metaPublished = Meta(Anonymous(), Clock.systemUTC.instant())

    "index a SchemaPublished event" in {
      val rev = 3L
      indexer(SchemaPublished(id, rev, metaPublished)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 19
      rs.toSet should contain theSameElementsAs expectedTriples(id,
                                                                rev,
                                                                deprecated = false,
                                                                published = true,
                                                                metaPublished,
                                                                meta,
                                                                "updated") ++ Set(
        (id.qualifyAsStringWith(schemasBase), publishedAtTimeKey, metaPublished.instant.toString))
    }

    "index a SchemaDeprecated event" in {
      val metaUpdated = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev         = 4L
      indexer(SchemaDeprecated(id, rev, metaUpdated)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 19
      rs.toSet should contain theSameElementsAs expectedTriples(id,
                                                                rev,
                                                                deprecated = true,
                                                                published = true,
                                                                metaUpdated,
                                                                meta,
                                                                "updated") ++ Set(
        (id.qualifyAsStringWith(schemasBase), publishedAtTimeKey, metaPublished.instant.toString))
    }
  }
}
