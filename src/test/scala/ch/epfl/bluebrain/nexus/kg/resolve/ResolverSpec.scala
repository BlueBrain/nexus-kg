package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import cats.implicits._
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.commons.test.{CirceEq, EitherValues, Resources}
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, Inspectors, OptionValues, TryValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class ResolverSpec
    extends AnyWordSpecLike
    with Matchers
    with Resources
    with EitherValues
    with OptionValues
    with IdiomaticMockito
    with BeforeAndAfter
    with TestHelper
    with TryValues
    with Inspectors
    with CirceEq {

  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  private implicit val projectCache = mock[ProjectCache[CId]]

  private implicit val iamClientConfig =
    IamClientConfig(url"http://example.com".value, url"http://example.com".value, "iam", 1.second)

  before {
    Mockito.reset(projectCache)
  }

  "A Resolver" when {
    val inProject        = jsonContentOf("/resolve/in-project.json").appendContextOf(resolverCtx)
    val crossProject     = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val crossProjectAnon = jsonContentOf("/resolve/cross-project3.json").appendContextOf(resolverCtx)
    val crossProjectRefs = jsonContentOf("/resolve/cross-project-refs.json").appendContextOf(resolverCtx)
    val iri              = Iri.absolute("http://example.com/id").rightValue
    val projectRef       = ProjectRef(genUUID)
    val id               = Id(projectRef, iri)
    val identities       = List[Identity](Group("bbp-ou-neuroinformatics", "ldap2"), User("dmontero", "ldap"))
    val label1           = ProjectLabel("account1", "project1")
    val label2           = ProjectLabel("account1", "project2")

    "constructing" should {

      "return an InProjectResolver" in {
        val resource = simpleV(id, inProject, types = Set(nxv.Resolver, nxv.InProject, nxv.Resource))
        Resolver(resource).rightValue shouldEqual InProjectResolver(
          projectRef,
          iri,
          resource.rev,
          resource.deprecated,
          10
        )
      }

      "return a CrossProjectResolver" in {
        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val projects = List(label1, label2)
        val resolver = Resolver(resource).rightValue.asInstanceOf[CrossProjectResolver[ProjectLabel]]
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
        Resolver(resource).rightValue.asInstanceOf[CrossProjectResolver[ProjectLabel]] shouldEqual
          CrossProjectResolver(
            Set(nxv.Schema.value),
            List(ProjectLabel("account1", "project1"), ProjectLabel("account1", "project2")),
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
        val resolver = Resolver(resource).rightValue.asInstanceOf[CrossProjectResolver[ProjectRef]]
        resolver.priority shouldEqual 50
        resolver.identities should contain theSameElementsAs identities
        resolver.resourceTypes shouldEqual Set.empty
        resolver.projects shouldEqual List(ProjectRef(UUID.fromString("ee9bb6e8-2bee-45f8-8a57-82e05fff0169")))
        resolver.ref shouldEqual projectRef
        resolver.id shouldEqual iri
        resolver.rev shouldEqual resource.rev
        resolver.deprecated shouldEqual resource.deprecated
      }

      "fail when the types don't match" in {
        val resource = simpleV(id, inProject, types = Set(nxv.Resource))
        Resolver(resource).toOption shouldEqual None
      }

      "fail when payload on identity is wrong" in {
        val invalid = List.range(1, 3).map(i => jsonContentOf(s"/resolve/cross-project-wrong-$i.json"))
        forAll(invalid) { invalidResolver =>
          val resource =
            simpleV(id, invalidResolver.appendContextOf(resolverCtx), types = Set(nxv.Resolver, nxv.CrossProject))
          Resolver(resource).toOption shouldEqual None
        }
      }
    }

    "converting into json " should {

      "return the json representation" in {

        val resolver: Resolver = CrossProjectResolver(
          Set(nxv.Schema.value),
          List(ProjectLabel("account1", "project1"), ProjectLabel("account1", "project2")),
          List(Anonymous),
          projectRef,
          iri,
          1L,
          false,
          50
        )

        val metadata = Json.obj(
          "_rev"        -> Json.fromLong(1L),
          "_deprecated" -> Json.fromBoolean(false),
          "identities" -> Json.arr(
            Json.obj(
              "@id"   -> Json.fromString("http://example.com/iam/anonymous"),
              "@type" -> Json.fromString("Anonymous")
            )
          )
        )
        val json = resolver.as[Json](resolverCtx.appendContextOf(resourceCtx)).rightValue.removeNestedKeys("@context")
        json should equalIgnoreArrayOrder(crossProjectAnon.removeNestedKeys("@context") deepMerge metadata)
      }
    }

    "converting" should {

      val uuid1 = genUUID
      val uuid2 = genUUID

      "generate a CrossProjectResolver" in {
        projectCache.getProjectRefs(List(label1, label2)) shouldReturn Map(
          label1 -> Option(ProjectRef(uuid1)),
          label2 -> Option(ProjectRef(uuid2))
        )
        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val exposed  = Resolver(resource).rightValue.asInstanceOf[CrossProjectResolver[ProjectLabel]]
        val stored   = exposed.referenced.value.rightValue.asInstanceOf[CrossProjectResolver[ProjectRef]]
        stored.priority shouldEqual 50
        stored.identities should contain theSameElementsAs identities
        stored.resourceTypes shouldEqual Set(nxv.Schema.value)
        stored.projects shouldEqual List(ProjectRef(uuid1), ProjectRef(uuid2))
        stored.ref shouldEqual projectRef
        stored.id shouldEqual iri
        stored.rev shouldEqual resource.rev
        stored.deprecated shouldEqual resource.deprecated
      }

      "generate a CrossProjectLabelResolver" in {
        projectCache.getProjectLabels(List(ProjectRef(uuid1), ProjectRef(uuid2))) shouldReturn
          Map(ProjectRef(uuid1) -> Option(label1), ProjectRef(uuid2) -> Option(label2))
        projectCache.getProjectRefs(List(label1, label2)) shouldReturn
          Map(label1 -> Option(ProjectRef(uuid1)), label2 -> Option(ProjectRef(uuid2)))

        val resource = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        val exposed  = Resolver(resource).rightValue.asInstanceOf[CrossProjectResolver[ProjectLabel]]
        val stored   = exposed.referenced.value.rightValue
        stored.labeled.value.rightValue.asInstanceOf[CrossProjectResolver[ProjectLabel]] shouldEqual exposed
      }
    }
  }

}
