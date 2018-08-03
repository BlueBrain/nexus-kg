package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.{Clock, Instant, ZoneId}

import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InAccountResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, Id, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import org.scalatest._

class ResolverSpec
    extends WordSpecLike
    with Matchers
    with Resources
    with EitherValues
    with OptionValues
    with TestHelper
    with TryValues {
  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  "A Resolver" when {
    val inProject    = jsonContentOf("/resolve/in-project.json").appendContextOf(resolverCtx)
    val crossProject = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val inAccount    = jsonContentOf("/resolve/in-account.json").appendContextOf(resolverCtx)
    val iri          = Iri.absolute("http://example.com/id").right.value
    val projectRef   = ProjectRef("ref")
    val id           = Id(projectRef, iri)
    val accountRef   = AccountRef("accountRef")
    val identities   = List[Identity](GroupRef("ldap2", "bbp-ou-neuroinformatics"), UserRef("ldap", "dmontero"))
    val flattenedCtx = resolverCtx.appendContextOf(resourceCtx)

    "constructing" should {

      "return an InProjectResolver" in {
        val resource = simpleV(id, inProject, types = Set(nxv.Resolver, nxv.InProject, nxv.Resource))
        Resolver(resource, accountRef).value shouldEqual InProjectResolver(projectRef,
                                                                           iri,
                                                                           resource.rev,
                                                                           resource.deprecated,
                                                                           10)
      }

      "return a CrossProjectResolver" in {
        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val projects =
          Set(ProjectRef("account1/project1"), ProjectRef("account1/project2"))
        val resolver = Resolver(resource, accountRef).value.asInstanceOf[CrossProjectResolver]
        resolver.priority shouldEqual 50
        resolver.identities should contain theSameElementsAs identities
        resolver.resourceTypes shouldEqual Set(nxv.Schema.value)
        resolver.projects shouldEqual projects
        resolver.ref shouldEqual projectRef
        resolver.id shouldEqual iri
        resolver.rev shouldEqual resource.rev
        resolver.deprecated shouldEqual resource.deprecated
      }

      "return a InAccountResolver" in {
        val resource = simpleV(id, inAccount, types = Set(nxv.Resolver, nxv.InAccount))
        val resolver = Resolver(resource, accountRef).value.asInstanceOf[InAccountResolver]
        resolver.priority shouldEqual 50
        resolver.identities should contain theSameElementsAs identities
        resolver.resourceTypes shouldEqual Set(nxv.Schema.value)
        resolver.ref shouldEqual projectRef
        resolver.id shouldEqual iri
        resolver.rev shouldEqual resource.rev
        resolver.deprecated shouldEqual resource.deprecated
      }

      "fail when the types don't match" in {
        val resource = simpleV(id, inProject, types = Set(nxv.Resource))
        Resolver(resource, accountRef) shouldEqual None
      }

      "fail when payload on identity is wrong" in {
        val wrong    = jsonContentOf("/resolve/cross-project-wrong.json").appendContextOf(resolverCtx)
        val resource = simpleV(id, wrong, types = Set(nxv.Resolver, nxv.CrossProject))
        Resolver(resource, accountRef) shouldEqual None
      }
    }

    "converting into json (from Graph)" should {

      "return the json representation for InProjectResolver" in {
        val resolver: Resolver = InProjectResolver(projectRef, iri, 1L, false, 10)
        resolver
          .asJson(flattenedCtx)
          .removeKeys("@context")
          .addContext(resolverCtxUri)
          .addContext(resourceCtxUri) shouldEqual jsonContentOf("/resolve/in-project-resp.json")
      }

      "return the json representation for CrossProjectResolver" in {
        val projects =
          Set(ProjectRef("account1/project1"), ProjectRef("account1/project2"))
        val resolver: Resolver = CrossProjectResolver(Set(nxv.Resolver, nxv.CrossProject),
                                                      projects,
                                                      identities,
                                                      projectRef,
                                                      iri,
                                                      1L,
                                                      false,
                                                      10)
        resolver
          .asJson(flattenedCtx)
          .removeKeys("@context")
          .addContext(resolverCtxUri)
          .addContext(resourceCtxUri) should equalIgnoreArrayOrder(jsonContentOf("/resolve/cross-project-resp.json"))
      }

      "return the json representation for InAccountResolver" in {
        val resolver: Resolver =
          InAccountResolver(Set(nxv.Resolver, nxv.InAccount), identities, accountRef, projectRef, iri, 1L, false, 10)
        resolver
          .asJson(flattenedCtx)
          .removeKeys("@context")
          .addContext(resolverCtxUri)
          .addContext(resourceCtxUri) should equalIgnoreArrayOrder(jsonContentOf("/resolve/in-account-resp.json"))
      }
    }
  }

}
