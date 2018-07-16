package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.{Clock, Instant, ZoneId}

import cats.data.OptionT
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.CrossProjectResolver
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import io.circe.Json
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class CrossProjectResolutionSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with MockitoSugar
    with Randomness
    with BeforeAndAfter
    with OptionValues {

  private implicit val clock: Clock              = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val resources: Resources[CId] = mock[Resources[CId]]
  private def genJson(): Json                    = Json.obj("key" -> Json.fromString(genString()))

  before {
    Mockito.reset(resources)
  }

  "A CrossProjectResolution" should {
    val proj        = ProjectRef("uuid1")
    val projects    = Set(ProjectRef("uuid2"), ProjectRef("uuid3"))
    val projects2   = Set(ProjectRef("uuid4"), ProjectRef("uuid5"))
    def iri(n: Int) = Iri.absolute(s"http://example.com/id$n").right.value
    val resId       = Iri.absolute("http://example.com/res-id").right.value

    "resolve a resource based on resolvers priority" in {
      val resolvers: List[Resolver] =
        List(
          CrossProjectResolver(Set(nxv.Schema.value), projects, List.empty, proj, iri(1), 1L, false, 10),
          CrossProjectResolver(Set(nxv.Schema.value), projects, List.empty, proj, iri(2), 1L, false, 50),
          CrossProjectResolver(Set(nxv.Schema.value), projects2, List.empty, proj, iri(3), 1L, false, 60),
          CrossProjectResolver(Set(nxv.Schema.value), projects2, List.empty, proj, iri(4), 1L, true, 0)
        )

      when(resources.fetch(Id(ProjectRef("uuid2"), resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(ProjectRef("uuid3"), resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(ProjectRef("uuid4"), resId), None)).thenReturn(OptionT.none[CId, Resource])

      val id    = Id(ProjectRef("uuid5"), resId)
      val value = simpleF(id, genJson(), types = Set(nxv.Schema, nxv.Resource))
      when(resources.fetch(id, None)).thenReturn(OptionT.some[CId](value))

      val resolution = CrossProjectResolution[CId](resources, resolvers)
      resolution.resolve(Latest(resId)).value shouldEqual value
      resolution.resolveAll(Latest(resId)) shouldEqual List(value)
    }

    "return none for a resource which no resolver resolves" in {
      val resolvers: List[Resolver] =
        List(
          CrossProjectResolver(Set(nxv.Schema.value), projects, List.empty, proj, iri(2), 1L, false, 50),
          CrossProjectResolver(Set(nxv.Schema.value), projects, List.empty, proj, iri(1), 1L, false, 10)
        )

      when(resources.fetch(Id(ProjectRef("uuid2"), resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(ProjectRef("uuid3"), resId), None)).thenReturn(OptionT.none[CId, Resource])

      val resolution = CrossProjectResolution[CId](resources, resolvers)
      resolution.resolve(Latest(resId)) shouldEqual None
      resolution.resolveAll(Latest(resId)) shouldEqual List.empty[Resource]
    }

    "return none for a resource which the resolver to resolve is deprecated" in {
      val resolvers: List[Resolver] =
        List(
          CrossProjectResolver(Set(nxv.Schema.value), projects2, List.empty, proj, iri(4), 1L, true, 0),
          CrossProjectResolver(Set(nxv.Schema.value), projects, List.empty, proj, iri(1), 1L, false, 10)
        )

      when(resources.fetch(Id(ProjectRef("uuid2"), resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(ProjectRef("uuid3"), resId), None)).thenReturn(OptionT.none[CId, Resource])

      val resolution = CrossProjectResolution[CId](resources, resolvers)
      resolution.resolve(Latest(resId)) shouldEqual None
      resolution.resolveAll(Latest(resId)) shouldEqual List.empty[Resource]

    }

    "return none for a resource in a project without resolvers" in {
      val resolution = CrossProjectResolution[CId](resources, List.empty)
      resolution.resolve(Latest(resId)) shouldEqual None
      resolution.resolveAll(Latest(resId)) shouldEqual List.empty[Resource]
    }

    "return a list of resources when the resource is present on several resolvers" in {
      val resolvers: List[Resolver] =
        List(
          CrossProjectResolver(Set(nxv.Schema.value), projects, List.empty, proj, iri(1), 1L, false, 100),
          CrossProjectResolver(Set(nxv.Schema.value), projects, List.empty, proj, iri(2), 1L, false, 12),
          CrossProjectResolver(Set(nxv.Schema.value), projects2, List.empty, proj, iri(3), 1L, false, 10)
        )

      when(resources.fetch(Id(ProjectRef("uuid2"), resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(ProjectRef("uuid3"), resId), None)).thenReturn(OptionT.none[CId, Resource])
      when(resources.fetch(Id(ProjectRef("uuid4"), resId), None)).thenReturn(OptionT.none[CId, Resource])

      val id1    = Id(ProjectRef("uuid5"), resId)
      val value1 = simpleF(id1, genJson(), types = Set(nxv.Schema, nxv.Resource))
      when(resources.fetch(id1, None)).thenReturn(OptionT.some[CId](value1))

      val id2    = Id(ProjectRef("uuid3"), resId)
      val value2 = simpleF(id2, genJson(), types = Set(nxv.Schema, nxv.Resource))
      when(resources.fetch(id2, None)).thenReturn(OptionT.some[CId](value2))

      val resolution = CrossProjectResolution[CId](resources, resolvers)
      resolution.resolveAll(Latest(resId)) shouldEqual List(value1, value2)
      verify(resources, times(1)).fetch(Id(ProjectRef("uuid2"), resId), None)
      verify(resources, times(1)).fetch(Id(ProjectRef("uuid3"), resId), None)
      verify(resources, times(1)).fetch(Id(ProjectRef("uuid4"), resId), None)
      verify(resources, times(1)).fetch(Id(ProjectRef("uuid5"), resId), None)

      resolution.resolve(Latest(resId)).value shouldEqual value1
    }

    "return none when the resource type does not match the resolver's expected type" in {
      val resolvers: List[Resolver] =
        List(CrossProjectResolver(Set(nxv.Schema.value), projects, List.empty, proj, iri(1), 1L, false, 10))

      when(resources.fetch(Id(ProjectRef("uuid2"), resId), None)).thenReturn(OptionT.none[CId, Resource])

      val id    = Id(ProjectRef("uuid3"), resId)
      val value = simpleF(id, genJson(), types = Set(nxv.Resource))
      when(resources.fetch(id, None)).thenReturn(OptionT.some[CId](value))

      val resolution = CrossProjectResolution[CId](resources, resolvers)
      resolution.resolve(Latest(resId)) shouldEqual None
      resolution.resolveAll(Latest(resId)) shouldEqual List.empty[Resource]
    }
  }

}
