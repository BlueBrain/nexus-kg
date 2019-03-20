package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatestplus.mockito.MockitoSugar

class ResolverSpec
    extends WordSpecLike
    with Matchers
    with Resources
    with EitherValues
    with OptionValues
    with MockitoSugar
    with BeforeAndAfter
    with TestHelper
    with TryValues {

  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  private implicit val projectCache = mock[ProjectCache[CId]]

  before {
    Mockito.reset(projectCache)
  }

  "A Resolver" when {
    val inProject        = jsonContentOf("/resolve/in-project.json").appendContextOf(resolverCtx)
    val crossProject     = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val crossProjectAnon = jsonContentOf("/resolve/cross-project3.json").appendContextOf(resolverCtx)
    val crossProjectRefs = jsonContentOf("/resolve/cross-project-refs.json").appendContextOf(resolverCtx)
    val iri              = Iri.absolute("http://example.com/id").right.value
    val projectRef       = ProjectRef(genUUID)
    val id               = Id(projectRef, iri)
    val identities       = List[Identity](Group("bbp-ou-neuroinformatics", "ldap2"), User("dmontero", "ldap"))
    val label1           = ProjectLabel("account1", "project1")
    val label2           = ProjectLabel("account1", "project2")

    "constructing" should {

      "return an InProjectResolver" in {
        val resource = simpleV(id, inProject, types = Set(nxv.Resolver, nxv.InProject, nxv.Resource))
        Resolver(resource).value shouldEqual InProjectResolver(projectRef, iri, resource.rev, resource.deprecated, 10)
      }

      "return a CrossProjectResolver" in {
        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val projects = Set(label1, label2)
        val resolver = Resolver(resource).value.asInstanceOf[CrossProjectResolver[ProjectLabel]]
        resolver.priority shouldEqual 50
        resolver.identities should contain theSameElementsAs identities
        resolver.resourceTypes shouldEqual Set(nxv.Schema.value)
        resolver.projects shouldEqual projects
        resolver.ref shouldEqual projectRef
        resolver.id shouldEqual iri
        resolver.rev shouldEqual resource.rev
        resolver.deprecated shouldEqual resource.deprecated
      }
      "return a CrossProjectResolver with anonymous identity" in {
        val resource = simpleV(id, crossProjectAnon, types = Set(nxv.Resolver, nxv.CrossProject))
        Resolver(resource).value.asInstanceOf[CrossProjectResolver[ProjectLabel]] shouldEqual
          CrossProjectResolver(
            Set(nxv.Schema.value),
            Set(ProjectLabel("account1", "project1"), ProjectLabel("account1", "project2")),
            List(Anonymous),
            projectRef,
            iri,
            resource.rev,
            resource.deprecated,
            50
          )

      }

      "return a CrossProjectResolver that does not have resourceTypes" in {
        val resource = simpleV(id, crossProjectRefs, types = Set(nxv.Resolver, nxv.CrossProject))
        val resolver = Resolver(resource).value.asInstanceOf[CrossProjectResolver[ProjectRef]]
        resolver.priority shouldEqual 50
        resolver.identities should contain theSameElementsAs identities
        resolver.resourceTypes shouldEqual Set.empty
        resolver.projects shouldEqual Set(ProjectRef(UUID.fromString("ee9bb6e8-2bee-45f8-8a57-82e05fff0169")))
        resolver.ref shouldEqual projectRef
        resolver.id shouldEqual iri
        resolver.rev shouldEqual resource.rev
        resolver.deprecated shouldEqual resource.deprecated
      }

      "fail when the types don't match" in {
        val resource = simpleV(id, inProject, types = Set(nxv.Resource))
        Resolver(resource) shouldEqual None
      }

      "fail when payload on identity is wrong" in {
        val wrong    = jsonContentOf("/resolve/cross-project-wrong-1.json").appendContextOf(resolverCtx)
        val resource = simpleV(id, wrong, types = Set(nxv.Resolver, nxv.CrossProject))
        Resolver(resource) shouldEqual None
      }
    }

    "converting into json (from Graph)" should {

      "return the list representation" in {
        val iri2 = Iri.absolute("http://example.com/id2").right.value

        val inProject: Resolver = InProjectResolver(projectRef, iri, 1L, false, 10)

        val crossProject: Resolver =
          CrossProjectResolver(
            Set(nxv.Resolver, nxv.CrossProject),
            Set(label1, label2),
            List(User("dmontero", "ldap")),
            projectRef,
            iri2,
            1L,
            false,
            11
          )
        val resolvers: QueryResults[Resolver] =
          QueryResults(2L, List(UnscoredQueryResult(inProject), UnscoredQueryResult(crossProject)))
        ResolverEncoder.json(resolvers).right.value shouldEqual jsonContentOf("/resolve/resolver-list-resp.json")
      }
    }

    "converting" should {

      val uuid1 = genUUID
      val uuid2 = genUUID

      "generate a CrossProjectResolver" in {
        when(projectCache.getProjectRefs(Set(label1, label2)))
          .thenReturn(Map(label1 -> Option(ProjectRef(uuid1)), label2 -> Option(ProjectRef(uuid2))))
        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val exposed  = Resolver(resource).value.asInstanceOf[CrossProjectResolver[ProjectLabel]]
        val stored   = exposed.referenced.value.right.value.asInstanceOf[CrossProjectResolver[ProjectRef]]
        stored.priority shouldEqual 50
        stored.identities should contain theSameElementsAs identities
        stored.resourceTypes shouldEqual Set(nxv.Schema.value)
        stored.projects shouldEqual Set(ProjectRef(uuid1), ProjectRef(uuid2))
        stored.ref shouldEqual projectRef
        stored.id shouldEqual iri
        stored.rev shouldEqual resource.rev
        stored.deprecated shouldEqual resource.deprecated
      }

      "generate a CrossProjectLabelResolver" in {
        when(projectCache.getProjectLabels(Set(ProjectRef(uuid1), ProjectRef(uuid2))))
          .thenReturn(Map(ProjectRef(uuid1) -> Option(label1), ProjectRef(uuid2) -> Option(label2)))
        when(projectCache.getProjectRefs(Set(label2, label1)))
          .thenReturn(Map(label1 -> Option(ProjectRef(uuid1)), label2 -> Option(ProjectRef(uuid2))))

        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val exposed  = Resolver(resource).value.asInstanceOf[CrossProjectResolver[ProjectLabel]]
        val stored   = exposed.referenced.value.right.value
        stored.labeled.value.right.value.asInstanceOf[CrossProjectResolver[ProjectLabel]] shouldEqual exposed
      }
    }
  }

}
