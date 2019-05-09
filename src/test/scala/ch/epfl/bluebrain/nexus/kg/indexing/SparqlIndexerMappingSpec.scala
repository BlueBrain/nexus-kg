package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.data.EitherT
import cats.effect.IO
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlWriteQuery
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import io.circe.Json
import org.mockito.Mockito._
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

import scala.concurrent.duration._

class SparqlIndexerMappingSpec
    extends TestKit(ActorSystem("SparqlIndexerMappingSpec"))
    with WordSpecLike
    with Matchers
    with IdiomaticMockito
    with IOEitherValues
    with IOOptionValues
    with test.Resources
    with BeforeAndAfter
    with TestHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 0.3 seconds)

  private val resources       = mock[Resources[IO]]
  private val projectRef      = ProjectRef(genUUID)
  private val organizationRef = OrganizationRef(genUUID)
  private implicit val project =
    Project(
      genIri,
      "some-label-proj",
      "some-label",
      None,
      nxv.project.value,
      genIri,
      Map(),
      projectRef.id,
      organizationRef.id,
      1L,
      false,
      Instant.EPOCH,
      genIri,
      Instant.EPOCH,
      genIri
    )

  before {
    Mockito.reset(resources)
  }

  "An Sparql event mapping function" when {

    val id: ResId = Id(ProjectRef(UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")),
                       url"https://bbp.epfl.ch/nexus/data/resourceName".value)

    val schema: Ref           = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)
    val json                  = Json.obj("key" -> Json.fromInt(2))
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val ev                    = Created(id, schema, Set.empty, json, clock.instant(), Anonymous)

    "using default view" should {

      val view   = SparqlView(Set.empty, Set.empty, None, true, true, id.parent, genIri, genUUID, 1L, deprecated = false)
      val mapper = new SparqlIndexerMapping(view, resources)

      "return none when the event resource is not found on the resources" in {
        when(resources.fetch(id, selfAsIri = true))
          .thenReturn(EitherT.leftT[IO, ResourceV](NotFound(id.ref): Rejection))
        mapper(ev).ioValue shouldEqual None
      }

      "return a SparqlWriteQuery" in {
        val resV = ResourceF.simpleV(id,
                                     ResourceF.Value(json, json.contextValue, RootedGraph(IriNode(id.value), Graph())),
                                     2L,
                                     schema = schema)
        when(resources.fetch(id, selfAsIri = true)).thenReturn(EitherT.rightT[IO, Rejection](resV))

        mapper(ev).some shouldEqual resV.id -> SparqlWriteQuery.replace(id.value.asString + "/graph", Graph())
      }
    }

    "using a view for a specific schema and tag" should {
      val view = SparqlView(
        Set(nxv.Resolver.value, nxv.Resource.value),
        Set.empty,
        Some("one"),
        includeMetadata = true,
        includeDeprecated = true,
        id.parent,
        nxv.defaultElasticSearchIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val mapper = new SparqlIndexerMapping(view, resources)

      "return none when the resource does not have the valid tag" in {
        resources.fetch(id, "one", selfAsIri = true) shouldReturn EitherT.leftT[IO, ResourceV](
          NotFound(Ref(genIri)): Rejection)
        mapper(ev).ioValue shouldEqual None
      }

      "return none when the schema is not on the view" in {
        val resV = ResourceF.simpleV(id,
                                     ResourceF.Value(json, json.contextValue, RootedGraph(IriNode(id.value), Graph())),
                                     2L,
                                     schema = schema)
        resources.fetch(id, "one", selfAsIri = true) shouldReturn EitherT.rightT[IO, Rejection](resV)
        mapper(ev).ioValue shouldEqual None
      }

      "return a SparqlWriteQuery" in {
        val resV = ResourceF
          .simpleV(id,
                   ResourceF.Value(json, json.contextValue, RootedGraph(IriNode(id.value), Graph())),
                   2L,
                   schema = Ref(nxv.Resource.value))
          .copy(tags = Map("one" -> 2L))
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = Ref(nxv.Resource.value)).copy(tags = Map("one" -> 2L))
        when(resources.fetch(id, "one", selfAsIri = true)).thenReturn(EitherT.rightT[IO, Rejection](resV))

        mapper(ev.copy(schema = Ref(nxv.Resource.value))).some shouldEqual res.id -> SparqlWriteQuery.replace(
          id.value.asString + "/graph",
          Graph())
      }
    }
  }

}
