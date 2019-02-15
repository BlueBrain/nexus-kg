package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.data.{EitherT, OptionT}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlWriteQuery
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

import scala.concurrent.duration._

class SparqlIndexerMappingSpec
    extends TestKit(ActorSystem("SparqlIndexerMappingSpec"))
    with WordSpecLike
    with Matchers
    with MockitoSugar
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

  private val mapper = new SparqlIndexerMapping(resources)

  before {
    Mockito.reset(resources)
  }

  "An Sparql event mapping function" should {

    val id: ResId = Id(ProjectRef(UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")),
                       url"https://bbp.epfl.ch/nexus/data/resourceName".value)
    val schema: Ref           = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)
    val json                  = Json.obj("key" -> Json.fromInt(2))
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val ev                    = Created(id, schema, Set.empty, json, clock.instant(), Anonymous)

    "return none when the event resource is not found on the resources" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.none[IO, Resource])
      mapper(ev).ioValue shouldEqual None
    }

    "return a SparqlWriteQuery" in {
      val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
      when(resources.fetch(id, None)).thenReturn(OptionT.some[IO](res))
      when(resources.materializeWithMeta(res)).thenReturn(EitherT.rightT[IO, Rejection](
        ResourceF.simpleV(id, ResourceF.Value(json, json.contextValue, Graph()), 2L, schema = schema)))

      mapper(ev).some shouldEqual res.id -> SparqlWriteQuery.replace(id.value.asString + "/graph", Graph())
    }
  }

}
