package ch.epfl.bluebrain.nexus.kg.indexing.contexts

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
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent.{
  ContextCreated,
  ContextDeprecated,
  ContextPublished,
  ContextUpdated
}
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, IndexerFixture, Qualifier}
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

@DoNotDiscover
class ContextIndexerSpec(blazegraphPort: Int)
    extends TestKit(ActorSystem("ContextIndexerSpec"))
    with IndexerFixture
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Randomness
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

  private val settings @ ContextIndexingSettings(index, contextsBase, contextsBaseNs, nexusVocBase) =
    ContextIndexingSettings(genString(length = 6), base, s"$base/contexts/graphs", s"$base/voc/nexus/core")

  private implicit val stringQualifier: ConfiguredQualifier[String]           = Qualifier.configured[String](nexusVocBase)
  private implicit val orgIdQualifier: ConfiguredQualifier[OrgId]             = Qualifier.configured[OrgId](base)
  private implicit val domainIdQualifier: ConfiguredQualifier[DomainId]       = Qualifier.configured[DomainId](base)
  private implicit val contextNameQualifier: ConfiguredQualifier[ContextName] = Qualifier.configured[ContextName](base)

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

  private def expectedTriples(id: ContextId,
                              rev: Long,
                              deprecated: Boolean,
                              published: Boolean,
                              meta: Meta,
                              firstReqMeta: Meta): Set[(String, String, String)] = {
    val qualifiedId = id.qualifyAsStringWith(contextsBase)
    Set(
      (qualifiedId, "rev" qualifyAsStringWith nexusVocBase, rev.toString),
      (qualifiedId, "deprecated" qualifyAsStringWith nexusVocBase, deprecated.toString),
      (qualifiedId, "published" qualifyAsStringWith nexusVocBase, published.toString),
      (qualifiedId, "organization" qualifyAsStringWith nexusVocBase, id.domainId.orgId.qualifyAsString),
      (qualifiedId, "domain" qualifyAsStringWith nexusVocBase, id.domainId.qualifyAsString),
      (qualifiedId, "name" qualifyAsStringWith nexusVocBase, id.name),
      (qualifiedId, createdAtTimeKey, firstReqMeta.instant.toString),
      (qualifiedId, updatedAtTimeKey, meta.instant.toString),
      (qualifiedId, contextGroupKey, id.contextName.qualifyAsString),
      (qualifiedId, rdfTypeKey, "Context".qualifyAsString),
      (qualifiedId, "version" qualifyAsStringWith nexusVocBase, id.version.show)
    )
  }

  "A ContextIndexer" should {

    val client = SparqlClient[Future](blazegraphBaseUri)

    val indexer = ContextIndexer(client, settings)

    val orgId    = OrgId(genId())
    val domainId = DomainId(orgId, genId())
    val id       = ContextId(domainId, genName(), genVersion())

    val meta = Meta(Anonymous(), Clock.systemUTC.instant())

    val replacements = Map(Pattern.quote("{{base}}") -> base.toString, Pattern.quote("{{context}}") -> id.show)
    "index a ContextCreated event" in {
      client.createIndex(index, properties).futureValue
      val rev  = 1L
      val data = jsonContentOf("/contexts/minimal.json", replacements)
      indexer(ContextCreated(id, rev, meta, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 11
      rs.toSet should contain theSameElementsAs expectedTriples(id,
                                                                rev,
                                                                deprecated = false,
                                                                published = false,
                                                                meta,
                                                                meta)
    }

    "index a ContextUpdated event" in {
      val metaUpdated = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev         = 2L
      val data        = jsonContentOf("/contexts/minimal.json", replacements)
      indexer(ContextUpdated(id, rev, metaUpdated, data)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 11
      rs.toSet should contain theSameElementsAs expectedTriples(id,
                                                                rev,
                                                                deprecated = false,
                                                                published = false,
                                                                metaUpdated,
                                                                meta)
    }
    val metaPublished = Meta(Anonymous(), Clock.systemUTC.instant())
    "index a ContextPublished event" in {
      val rev = 3L
      indexer(ContextPublished(id, rev, metaPublished)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 12
      rs.toSet should contain theSameElementsAs expectedTriples(id,
                                                                rev,
                                                                deprecated = false,
                                                                published = true,
                                                                metaPublished,
                                                                meta) ++ Set(
        (id.qualifyAsStringWith(contextsBase), publishedAtTimeKey, metaPublished.instant.toString))
    }

    "index a ContextDeprecated event" in {
      val metaUpdated = Meta(Anonymous(), Clock.systemUTC.instant())
      val rev         = 4L
      indexer(ContextDeprecated(id, rev, metaUpdated)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 12
      rs.toSet should contain theSameElementsAs expectedTriples(id,
                                                                rev,
                                                                deprecated = true,
                                                                published = true,
                                                                metaUpdated,
                                                                meta) ++ Set(
        (id.qualifyAsStringWith(contextsBase), publishedAtTimeKey, metaPublished.instant.toString))
    }
  }

}
