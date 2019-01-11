package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.{Clock, Instant}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import cats.data.OptionT
import cats.instances.future._
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Group, User}
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.simpleF
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import io.circe.Json
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar

import scala.collection.immutable.ListSet
import scala.concurrent.Future
import scala.concurrent.duration._

class MultiProjectResolutionSpec
    extends TestKit(ActorSystem("MultiProjectResolutionSpec"))
    with WordSpecLike
    with Matchers
    with MockitoSugar
    with Randomness
    with BeforeAndAfter
    with EitherValues
    with TestHelper
    with OptionValues
    with BeforeAndAfterAll
    with ScalaFutures {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(6 seconds, 100 millis)

  private def genProjectLabel = ProjectLabel(genString(), genString())
  private def genJson: Json   = Json.obj("key" -> Json.fromString(genString()))

  private implicit val clock: Clock = Clock.systemUTC

  private val resources   = mock[Resources[Future]]
  private val managePerms = Set(Permission.unsafe("resources/manage"))

  private val base  = Iri.absolute("https://nexus.example.com").getOrElse(fail)
  private val resId = base + "some-id"
  private val (proj1Id, proj2Id, proj3Id) =
    (genUUID, genUUID, genUUID)
  private val (proj1, proj2, proj3) = (genProjectLabel, genProjectLabel, genProjectLabel)
  private val projects              = Future.successful(ListSet(proj1Id, proj2Id, proj3Id).map(ProjectRef(_))) // we want to ensure traversal order
  private val types                 = Set(nxv.Schema.value, nxv.Resource.value)
  private val group                 = Group("bbp-ou-neuroinformatics", "ldap2")
  private val identities            = List[Identity](group, User("dmontero", "ldap"))
  implicit val timeout              = Timeout(1 second)
  implicit val ec                   = system.dispatcher
  private val cache                 = DistributedCache.future()
  val acls                          = AccessControlLists(/ -> resourceAcls(AccessControlList(group -> managePerms)))

  private val resolution = MultiProjectResolution[Future](resources, projects, types, identities, cache, acls)

  override def beforeAll(): Unit = {
    super.beforeAll()
    List(proj1 -> proj1Id, proj2 -> proj2Id, proj3 -> proj3Id).foreach {
      case (proj, id) =>
        val organizationUuid = genUUID
        val metadata = Project(genIri,
                               proj.value,
                               proj.organization,
                               None,
                               base,
                               genIri,
                               Map(),
                               id,
                               organizationUuid,
                               0L,
                               false,
                               Instant.EPOCH,
                               genIri,
                               Instant.EPOCH,
                               genIri)
        cache
          .addOrganization(
            OrganizationRef(organizationUuid),
            Organization(genIri,
                         proj.organization,
                         "description",
                         organizationUuid,
                         1L,
                         false,
                         Instant.EPOCH,
                         genIri,
                         Instant.EPOCH,
                         genIri)
          )
          .futureValue
        cache.addProject(ProjectRef(id), OrganizationRef(organizationUuid), metadata).futureValue
    }
  }

  before(Mockito.reset(resources))

  "A MultiProjectResolution" should {
    val (id1, id2, id3) =
      (Id(ProjectRef(proj1Id), resId), Id(ProjectRef(proj2Id), resId), Id(ProjectRef(proj3Id), resId))
    "look in all projects to resolve a resource" in {
      val value = simpleF(id3, genJson, types = types)
      when(resources.fetch(id1, None)).thenReturn(OptionT.none[Future, Resource])
      when(resources.fetch(id2, None)).thenReturn(OptionT.none[Future, Resource])
      when(resources.fetch(id3, None)).thenReturn(OptionT.some[Future](value))

      resolution.resolve(Latest(resId)).futureValue.value shouldEqual value
      verify(resources, times(1)).fetch(id1, None)
      verify(resources, times(1)).fetch(id2, None)
    }

    "filter results according to the resolvers' resource types" in {
      val value1 = simpleF(id1, genJson)
      val value2 = simpleF(id2, genJson, types = Set(nxv.Schema.value))
      val value3 = simpleF(id3, genJson, types = Set(nxv.Ontology.value))
      List(id1 -> value1, id2 -> value2, id3 -> value3).foreach {
        case (id, value) => when(resources.fetch(id, None)).thenReturn(OptionT.some[Future](value))
      }

      resolution.resolve(Latest(resId)).futureValue shouldEqual Some(value2)
    }

    "filter results according to the resolvers' identities" in {
      val List(_, value2, _) = List(id1, id2, id3).map { id =>
        val value = simpleF(id, genJson, types = types)
        when(resources.fetch(id, None)).thenReturn(OptionT.some[Future](value))
        value
      }
      val acl =
        AccessControlLists(proj2.organization / proj2.value -> resourceAcls(AccessControlList(group -> managePerms)))

      val newResolution = MultiProjectResolution[Future](resources, projects, types, identities, cache, acl)

      newResolution.resolve(Latest(resId)).futureValue shouldEqual Some(value2)
    }

    "return none if the resource is not found in any project" in {
      List(proj1Id, proj2Id, proj3Id).foreach { id =>
        when(resources.fetch(Id(ProjectRef(id), resId), None)).thenReturn(OptionT.none[Future, Resource])
      }
      resolution.resolve(Latest(resId)).futureValue shouldEqual None
    }
  }
}
