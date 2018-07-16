package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.Clock

import cats.data.OptionT
import cats.{Applicative, Id => CId}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.simpleF
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import io.circe.Json
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class InAccountResolutionSpec
    extends WordSpecLike
    with Matchers
    with MockitoSugar
    with Randomness
    with BeforeAndAfter
    with EitherValues
    with OptionValues {

  private def genUUID: String = java.util.UUID.randomUUID.toString
  private def genJson: Json   = Json.obj("key" -> Json.fromString(genString()))

  private implicit val clock: Clock = Clock.systemUTC
  private val resources             = mock[Resources[CId]]
  private val cache                 = mock[DistributedCache[CId]]
  private val base                  = Iri.absolute("https://nexus.example.com").getOrElse(fail)
  private val resId                 = base + "/some-id"
  private val accountRef            = AccountRef(genUUID)
  private val resolution            = InAccountResolution(accountRef, resources, cache)
  private val (proj1, proj2, proj3) = (ProjectRef(genUUID), ProjectRef(genUUID), ProjectRef(genUUID))
  private val projects              = Set(proj1, proj2, proj3)

  before {
    Mockito.reset(resources)
    Mockito.reset(cache)
    when(cache.projects(accountRef)).thenReturn(Applicative[CId].pure(projects))
  }

  "An InAccountResolution" should {
    "look in all projects to resolve a resource" in {
      when(resources.fetch(Id(proj1, resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(proj2, resId), None)).thenReturn(OptionT.none[CId, Resource])

      val id    = Id(proj3, resId)
      val value = simpleF(id, genJson)
      when(resources.fetch(id, None)).thenReturn(OptionT.some[CId](value))

      resolution.resolve(Latest(resId)).value shouldEqual value
      resolution.resolveAll(Latest(resId)) shouldEqual List(value)
    }

    "look in all projects to resolve all resources" in {
      val id1    = Id(proj1, resId)
      val value1 = simpleF(id1, genJson)
      when(resources.fetch(id1, None)).thenReturn(OptionT.some[CId](value1))

      val id2    = Id(proj2, resId)
      val value2 = simpleF(id2, genJson)
      when(resources.fetch(id2, None)).thenReturn(OptionT.some[CId](value2))

      val id3    = Id(proj3, resId)
      val value3 = simpleF(id3, genJson)
      when(resources.fetch(id3, None)).thenReturn(OptionT.some[CId](value3))

      // flipped because of a scalatest bug with -Ypartial-unification
      List(value1, value2, value3) should contain theSameElementsAs resolution.resolveAll(Latest(resId))
    }

    "return none if the resource is not found in any project" in {
      when(resources.fetch(Id(proj1, resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(proj2, resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(proj3, resId), None)).thenReturn(OptionT.none[CId, Resource])
      resolution.resolve(Latest(resId)) shouldEqual None
      resolution.resolveAll(Latest(resId)) shouldEqual Nil
    }
  }
}
