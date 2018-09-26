package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.{Clock, Instant, ZoneId}

import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, Id, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import io.circe.syntax._
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

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

  private implicit val cache = mock[DistributedCache[CId]]

  before {
    Mockito.reset(cache)
  }

  "A Resolver" when {
    val inProject    = jsonContentOf("/resolve/in-project.json").appendContextOf(resolverCtx)
    val crossProject = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val inAccount    = jsonContentOf("/resolve/in-account.json").appendContextOf(resolverCtx)
    val iri          = Iri.absolute("http://example.com/id").right.value
    val projectRef   = ProjectRef("ref")
    val id           = Id(projectRef, iri)
    val accountRef   = AccountRef("accountRef")
    val identities   = List[Identity](GroupRef("ldap2", "bbp-ou-neuroinformatics"), UserRef("ldap", "dmontero"))

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
        val projects = Set(ProjectLabel("account1", "project1"), ProjectLabel("account1", "project2"))
        val resolver = Resolver(resource, accountRef).value.asInstanceOf[CrossProjectLabels]
        resolver.priority shouldEqual 50
        resolver.identities should contain theSameElementsAs identities
        resolver.resourceTypes shouldEqual Set(nxv.Schema.value)
        resolver.projects shouldEqual projects
        resolver.ref shouldEqual projectRef
        resolver.id shouldEqual iri
        resolver.rev shouldEqual resource.rev
        resolver.deprecated shouldEqual resource.deprecated
      }

      "return a CrossProjectResolver that does not have resourceTypes" in {
        val resource =
          simpleV(id, crossProject.removeKeys("resourceTypes"), types = Set(nxv.Resolver, nxv.CrossProject))
        val projects = Set(ProjectLabel("account1", "project1"), ProjectLabel("account1", "project2"))
        val resolver = Resolver(resource, accountRef).value.asInstanceOf[CrossProjectLabels]
        resolver.priority shouldEqual 50
        resolver.identities should contain theSameElementsAs identities
        resolver.resourceTypes shouldEqual Set.empty
        resolver.projects shouldEqual projects
        resolver.ref shouldEqual projectRef
        resolver.id shouldEqual iri
        resolver.rev shouldEqual resource.rev
        resolver.deprecated shouldEqual resource.deprecated
      }

      "return an InAccountResolver" in {
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

      "return a InAccountResolver that does not have resourceTypes" in {
        val resource = simpleV(id, inAccount.removeKeys("resourceTypes"), types = Set(nxv.Resolver, nxv.InAccount))
        val resolver = Resolver(resource, accountRef).value.asInstanceOf[InAccountResolver]
        resolver.priority shouldEqual 50
        resolver.identities should contain theSameElementsAs identities
        resolver.resourceTypes shouldEqual Set.empty
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
        val wrong    = jsonContentOf("/resolve/cross-project-wrong-1.json").appendContextOf(resolverCtx)
        val resource = simpleV(id, wrong, types = Set(nxv.Resolver, nxv.CrossProject))
        Resolver(resource, accountRef) shouldEqual None
      }
    }

    "converting into json (from Graph)" should {

      "return the list representation" in {
        val iri2 = Iri.absolute("http://example.com/id2").right.value
        val iri3 = Iri.absolute("http://example.com/id3").right.value

        val inProject: Resolver = InProjectResolver(projectRef, iri, 1L, false, 10)

        val crossProject: Resolver =
          CrossProjectResolver(
            Set(nxv.Resolver, nxv.CrossProject),
            Set(ProjectLabel("account1", "project1"), ProjectLabel("account1", "project2")),
            identities,
            projectRef,
            iri2,
            1L,
            false,
            10
          )
        val inAccount: Resolver =
          InAccountResolver(Set(nxv.Resolver, nxv.InAccount),
                            List(Anonymous, AuthenticatedRef(Some("some"))),
                            accountRef,
                            projectRef,
                            iri3,
                            1L,
                            false,
                            10)

        val resolvers: QueryResults[Resolver] = QueryResults(2L,
                                                             List(UnscoredQueryResult(inProject),
                                                                  UnscoredQueryResult(crossProject),
                                                                  UnscoredQueryResult(inAccount)))
        resolvers.asJson should equalIgnoreArrayOrder(jsonContentOf("/resolve/resolver-list-resp.json"))
      }
    }

    "converting" should {

      "generate a CrossProjectResolver" in {
        when(cache.projectRef(ProjectLabel("account1", "project1"))).thenReturn(Option(ProjectRef("uuid1")))
        when(cache.projectRef(ProjectLabel("account1", "project2"))).thenReturn(Option(ProjectRef("uuid2")))
        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val exposed  = Resolver(resource, accountRef).value.asInstanceOf[CrossProjectLabels]
        val stored   = exposed.referenced.value.right.value.asInstanceOf[CrossProjectRefs]
        stored.priority shouldEqual 50
        stored.identities should contain theSameElementsAs identities
        stored.resourceTypes shouldEqual Set(nxv.Schema.value)
        stored.projects shouldEqual Set(ProjectRef("uuid1"), ProjectRef("uuid2"))
        stored.ref shouldEqual projectRef
        stored.id shouldEqual iri
        stored.rev shouldEqual resource.rev
        stored.deprecated shouldEqual resource.deprecated
      }

      "generate a CrossProjectLabelResolver" in {
        when(cache.projectRef(ProjectLabel("account1", "project1"))).thenReturn(Option(ProjectRef("uuid1")))
        when(cache.projectRef(ProjectLabel("account1", "project2"))).thenReturn(Option(ProjectRef("uuid2")))
        List(
          (ProjectRef("uuid1"), ProjectLabel("account1", "project1"), AccountRef("uuid1Ac")),
          (ProjectRef("uuid2"), ProjectLabel("account1", "project2"), AccountRef("uuid2Ac"))
        ).map {
          case (projectRef, labels, accountRef) =>
            when(cache.accountRef(projectRef)).thenReturn(Option(accountRef))
            when(cache.account(accountRef))
              .thenReturn(Option(Account("name", 1L, labels.account, false, accountRef.id)))
            when(cache.project(projectRef)).thenReturn(
              Option(Project("name", labels.value, Map.empty, url"http://example.com", 1L, false, projectRef.id)))
        }

        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val exposed  = Resolver(resource, accountRef).value.asInstanceOf[CrossProjectLabels]
        val stored   = exposed.referenced.value.right.value
        stored.labeled.value.right.value.asInstanceOf[CrossProjectLabels] shouldEqual exposed
      }
    }
  }

}
