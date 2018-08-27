package ch.epfl.bluebrain.nexus.kg.indexing

import java.io.ByteArrayInputStream
import java.time.{Clock, Instant, ZoneId}
import java.util.regex.Pattern.quote

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.data.{EitherT, OptionT}
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.akka.iri._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json
import org.apache.jena.query.{ResultSet, ResultSetFactory}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class SparqlIndexerSpec
    extends TestKit(ActorSystem("SparqlIndexerSpec"))
    with WordSpecLike
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with test.Resources
    with BeforeAndAfter
    with TestHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 0.3 seconds)

  import system.dispatcher

  private val resources  = mock[Resources[Future]]
  private val client     = mock[SparqlClient[Future]]
  private val projectRef = ProjectRef(uuid)
  private val accountRef = AccountRef(uuid)
  private val project =
    Project("some-project", "some-label-proj", Map.empty, nxv.project.value, 1L, deprecated = false, projectRef.id)
  private val account                 = Account("some-org", 1L, "some-label", deprecated = false, accountRef.id)
  private implicit val labeledProject = LabeledProject(ProjectLabel(account.label, project.label), project, accountRef)
  private val indexer                 = new SparqlIndexer(client, resources)

  before {
    Mockito.reset(resources)
    Mockito.reset(client)
  }

  "An SparqlIndexer" should {
    val id: ResId             = Id(ProjectRef("org/projectName"), url"https://bbp.epfl.ch/nexus/data/resourceName".value)
    val schema: Ref           = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)
    val json                  = Json.obj("key" -> Json.fromInt(2))
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val ev                    = Created(id, 2L, schema, Set.empty, json, clock.instant(), Anonymous)

    def queryId =
      s"SELECT ?o WHERE {<${id.value.show}> <${nxv.rev.value.show}> ?o} LIMIT 1"

    def queryResp(rev: Long): ResultSet =
      ResultSetFactory.fromJSON(
        new ByteArrayInputStream(
          contentOf("/indexing/sparql-rev-response.json", Map(quote("{rev}") -> rev.toString)).getBytes))

    def queryEmptyResp: ResultSet =
      ResultSetFactory.fromJSON(new ByteArrayInputStream(contentOf("/indexing/sparql-rev-response-none.json").getBytes))

    "not index a resource when it exists a higher revision on the indexer" in {
      val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
      when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
      when(client.queryRs(queryId)).thenReturn(Future.successful(queryResp(3L)))
      indexer(ev).futureValue shouldEqual (())
    }

    "throw when the event resource is not found on the resources" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.none[Future, Resource])
      whenReady(indexer(ev).failed)(_ shouldEqual NotFound(id.ref))
    }

    "index a resource when it does not exist" in {
      val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
      when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
      when(client.queryRs(queryId)).thenReturn(Future.successful(queryEmptyResp))
      when(resources.materializeWithMeta(res)).thenReturn(EitherT.rightT[Future, Rejection](
        ResourceF.simpleV(id, ResourceF.Value(json, json.contextValue, Graph()), 2L, schema = schema)))

      when(client.replace(id.value + "graph", Graph())).thenReturn(Future.successful(()))

      indexer(ev).futureValue shouldEqual (())
      verify(client, times(1)).replace(id.value + "graph", Graph())

    }

    "index a resource when it exists a lower revision on the indexer" in {
      val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
      when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
      when(client.queryRs(queryId)).thenReturn(Future.successful(queryResp(1L)))

      when(resources.materializeWithMeta(res)).thenReturn(EitherT.rightT[Future, Rejection](
        ResourceF.simpleV(id, ResourceF.Value(json, json.contextValue, Graph()), 2L, schema = schema)))

      when(client.replace(id.value + "graph", Graph())).thenReturn(Future.successful(()))

      indexer(ev).futureValue shouldEqual (())
      verify(client, times(1)).replace(id.value + "graph", Graph())

    }
  }

}
