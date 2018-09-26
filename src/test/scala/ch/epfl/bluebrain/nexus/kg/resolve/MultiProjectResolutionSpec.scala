package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.Clock
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import cats.data.OptionT
import cats.instances.future._
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.simpleF
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import io.circe.Json
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import ch.epfl.bluebrain.nexus.iam.client.types.Address._

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
    with OptionValues
    with BeforeAndAfterAll
    with ScalaFutures {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(6 seconds, 100 millis)

  private def genProjectLabel = ProjectLabel(genString(), genString())
  private def genJson: Json   = Json.obj("key" -> Json.fromString(genString()))

  private implicit val clock: Clock = Clock.systemUTC

  private val resources = mock[Resources[Future]]

  private val base  = Iri.absolute("https://nexus.example.com").getOrElse(fail)
  private val resId = base + "some-id"
  private val (proj1Id, proj2Id, proj3Id) =
    (UUID.randomUUID().toString, UUID.randomUUID().toString, UUID.randomUUID().toString)
  private val (proj1, proj2, proj3) = (genProjectLabel, genProjectLabel, genProjectLabel)
  private val projects              = Future.successful(ListSet(proj1Id, proj2Id, proj3Id).map(_.ref)) // we want to ensure traversal order
  private val types                 = Set(nxv.Schema.value, nxv.Resource.value)
  private val group                 = GroupRef("ldap2", "bbp-ou-neuroinformatics")
  private val identities            = List[Identity](group, UserRef("ldap", "dmontero"))
  implicit val timeout              = Timeout(1 second)
  implicit val ec                   = system.dispatcher
  private val cache                 = DistributedCache.future()
  val acls                          = FullAccessControlList((group, /, Permissions(Permission("resources/manage"))))
  private val resolution            = MultiProjectResolution[Future](resources, projects, types, identities, cache, acls)

  override def beforeAll(): Unit = {
    super.beforeAll()
    List(proj1 -> proj1Id, proj2 -> proj2Id, proj3 -> proj3Id).foreach {
      case (proj, id) =>
        val metadata = Project(proj.value, proj.value, Map(), base, 1L, false, id)
        val uuid     = UUID.randomUUID().toString
        cache.addAccount(AccountRef(uuid), Account(proj.account, 1L, proj.account, false, uuid)).futureValue
        cache.addProject(id.ref, AccountRef(uuid), metadata).futureValue
    }
  }

  before(Mockito.reset(resources))

  "A MultiProjectResolution" should {
    val (id1, id2, id3) = (Id(proj1Id.ref, resId), Id(proj2Id.ref, resId), Id(proj3Id.ref, resId))
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

      val acl           = FullAccessControlList((group, proj2.account / proj2.value, Permissions(Permission("resources/manage"))))
      val newResolution = MultiProjectResolution[Future](resources, projects, types, identities, cache, acl)

      newResolution.resolve(Latest(resId)).futureValue shouldEqual Some(value2)
    }

    "return none if the resource is not found in any project" in {
      List(proj1Id, proj2Id, proj3Id).foreach { id =>
        when(resources.fetch(Id(id.ref, resId), None)).thenReturn(OptionT.none[Future, Resource])
      }
      resolution.resolve(Latest(resId)).futureValue shouldEqual None
    }
  }

  private implicit class ProjectOps(uuid: String) {
    def ref = ProjectRef(uuid)
  }
}
