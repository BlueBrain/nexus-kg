package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.{Clock, Instant, ZoneId}

import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef, ResourceF}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import org.scalatest.{EitherValues, Matchers, OptionValues, WordSpecLike}

class ResolverSpec extends WordSpecLike with Matchers with Resources with EitherValues with OptionValues {
  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  "A Resolver" should {
    val inProject    = jsonContentOf("/resolve/in-project.json").appendContextOf(resolverCtx)
    val crossProject = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val iri          = Iri.absolute("http://example.com/id").right.value
    val projectRef   = ProjectRef("ref")
    val id           = Id(projectRef, iri)

    "construct a in project resolver" in {
      val resource = ResourceF.simpleV(id, inProject, types = Set(nxv.Resolver, nxv.InProject, nxv.Resource))
      Resolver(resource).value shouldEqual InProjectResolver(projectRef, iri, resource.rev, resource.deprecated, 10)
    }

    "construct a cross project resolver" in {
      val resource = ResourceF.simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
      val projects =
        Set(ProjectRef("70eab995-fc68-4abf-8493-8d5248ba1b18"), ProjectRef("bd024b643-84e0-4188-aa62-898aa84387d0"))
      val identities = List[Identity](GroupRef("ldap2", "bbp-ou-neuroinformatics"), UserRef("ldap", "dmontero"))
      val resolver   = Resolver(resource).value.asInstanceOf[CrossProjectResolver]
      resolver.priority shouldEqual 50
      resolver.identities should contain theSameElementsAs identities
      resolver.resourceTypes shouldEqual Set(nxv.Schema.value)
      resolver.projects shouldEqual projects
      resolver.ref shouldEqual projectRef
      resolver.id shouldEqual iri
      resolver.rev shouldEqual resource.rev
      resolver.deprecated shouldEqual resource.deprecated
    }

    "fail to construct when the types don't match" in {
      val resource = ResourceF.simpleV(id, inProject, types = Set(nxv.Resource))
      Resolver(resource) shouldEqual None
    }

    "fail to construct when payload on identity is wrong" in {
      val wrong    = jsonContentOf("/resolve/cross-project-wrong.json").appendContextOf(resolverCtx)
      val resource = ResourceF.simpleV(id, wrong, types = Set(nxv.Resolver, nxv.CrossProject))
      Resolver(resource) shouldEqual None
    }
  }

}
