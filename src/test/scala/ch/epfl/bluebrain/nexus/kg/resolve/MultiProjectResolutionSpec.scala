package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.Clock

import cats.data.OptionT
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.simpleF
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import io.circe.Json
import org.mockito.ArgumentMatchers.{anyString, eq => is}
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.collection.immutable.ListSet

class MultiProjectResolutionSpec
    extends WordSpecLike
    with Matchers
    with MockitoSugar
    with Randomness
    with BeforeAndAfter
    with EitherValues
    with OptionValues {

  private def genProjectLabel = ProjectLabel(genString(), genString())
  private def genJson: Json   = Json.obj("key" -> Json.fromString(genString()))

  private implicit val clock: Clock               = Clock.systemUTC
  private implicit val saToken: Option[AuthToken] = Some(AuthToken("service-account-token"))

  private val resources   = mock[Resources[CId]]
  private val adminClient = mock[AdminClient[CId]]

  private val base                  = Iri.absolute("https://nexus.example.com").getOrElse(fail)
  private val resId                 = base + "some-id"
  private val (proj1, proj2, proj3) = (genProjectLabel, genProjectLabel, genProjectLabel)
  private val projects              = ListSet(proj1, proj2, proj3).map(_.ref) // we want to ensure traversal order
  private val types                 = Set(nxv.Schema.value, nxv.Resource.value)
  private val group                 = GroupRef("ldap2", "bbp-ou-neuroinformatics")
  private val identities            = List[Identity](group, UserRef("ldap", "dmontero"))
  private val resolution            = MultiProjectResolution[CId](resources, projects, types, identities, adminClient)

  before {
    Mockito.reset(resources)
    when(adminClient.getProjectAcls(anyString, anyString, is(true), is(false))(is(saToken)))
      .thenReturn(
        Some(FullAccessControlList((group, Address("some/path"), Permissions(Permission("resources/manage"))))))
  }

  "A MultiProjectResolution" should {

    "look in all projects to resolve a resource" in {
      when(resources.fetch(Id(proj1.ref, resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(proj2.ref, resId), None)).thenReturn(OptionT.none[CId, Resource])

      val id    = Id(proj3.ref, resId)
      val value = simpleF(id, genJson, types = types)
      when(resources.fetch(id, None)).thenReturn(OptionT.some[CId](value))

      resolution.resolve(Latest(resId)).value shouldEqual value
      resolution.resolveAll(Latest(resId)) shouldEqual List(value)
    }

    "look in all projects to resolve all resources" in {
      val id1    = Id(proj1.ref, resId)
      val value1 = simpleF(id1, genJson, types = types)
      when(resources.fetch(id1, None)).thenReturn(OptionT.some[CId](value1))

      val id2    = Id(proj2.ref, resId)
      val value2 = simpleF(id2, genJson, types = types)
      when(resources.fetch(id2, None)).thenReturn(OptionT.some[CId](value2))

      val id3    = Id(proj3.ref, resId)
      val value3 = simpleF(id3, genJson, types = types)
      when(resources.fetch(id3, None)).thenReturn(OptionT.some[CId](value3))

      resolution.resolveAll(Latest(resId)) shouldEqual List(value1, value2, value3)
    }

    "filter results according to the resolvers' resource types" in {
      val id1    = Id(proj1.ref, resId)
      val value1 = simpleF(id1, genJson)
      when(resources.fetch(id1, None)).thenReturn(OptionT.some[CId](value1))

      val id2    = Id(proj2.ref, resId)
      val value2 = simpleF(id2, genJson, types = Set(nxv.Schema.value))
      when(resources.fetch(id2, None)).thenReturn(OptionT.some[CId](value2))

      val id3    = Id(proj3.ref, resId)
      val value3 = simpleF(id3, genJson, types = Set(nxv.Ontology.value))
      when(resources.fetch(id3, None)).thenReturn(OptionT.some[CId](value3))

      resolution.resolve(Latest(resId)) shouldEqual Some(value2)
      verify(resources, times(0)).fetch(id3, None)
      resolution.resolveAll(Latest(resId)) shouldEqual List(value2)
      verify(resources, times(1)).fetch(id3, None)
    }

    "return none if the resource is not found in any project" in {
      when(resources.fetch(Id(proj1.ref, resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(proj2.ref, resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(proj3.ref, resId), None)).thenReturn(OptionT.none[CId, Resource])
      resolution.resolve(Latest(resId)) shouldEqual None
      resolution.resolveAll(Latest(resId)) shouldEqual Nil
    }
  }

  implicit class ProjectOps(label: ProjectLabel) {
    def ref = ProjectRef(s"${label.account}/${label.value}")
  }
}
