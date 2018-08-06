package ch.epfl.bluebrain.nexus.kg.resolve

import java.time.Clock
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import cats.data.OptionT
import cats.instances.future._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
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
import org.mockito.ArgumentMatchers.{anyString, eq => is}
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
    with OptionValues
    with BeforeAndAfterAll
    with ScalaFutures {

  private def genProjectLabel = ProjectLabel(genString(), genString())
  private def genJson: Json   = Json.obj("key" -> Json.fromString(genString()))

  private implicit val clock: Clock               = Clock.systemUTC
  private implicit val saToken: Option[AuthToken] = Some(AuthToken("service-account-token"))

  private val resources   = mock[Resources[Future]]
  private val adminClient = mock[AdminClient[Future]]

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

  private val resolution = MultiProjectResolution[Future](resources, projects, types, identities, adminClient, cache)

  override def beforeAll(): Unit = {
    super.beforeAll()
    val account1Uuid = UUID.randomUUID().toString
    cache
      .addAccount(AccountRef(account1Uuid), Account(proj1.account, 1L, proj1.account, false, account1Uuid), true)
      .futureValue
    cache
      .addProject(proj1Id.ref,
                  AccountRef(account1Uuid),
                  Project(proj1.value, proj1.value, Map(), base, 1L, false, proj1Id),
                  true)
      .futureValue

    val account2Uuid = UUID.randomUUID().toString
    cache
      .addAccount(AccountRef(account2Uuid), Account(proj1.account, 1L, proj2.account, false, account2Uuid), true)
      .futureValue
    cache
      .addProject(proj2Id.ref,
                  AccountRef(account2Uuid),
                  Project(proj2.value, proj2.value, Map(), base, 1L, false, proj2Id),
                  true)
      .futureValue

    val account3Uuid = UUID.randomUUID().toString
    cache
      .addAccount(AccountRef(account3Uuid), Account(proj3.account, 1L, proj3.account, false, account3Uuid), true)
      .futureValue
    val _ = cache
      .addProject(proj3Id.ref,
                  AccountRef(account3Uuid),
                  Project(proj3.value, proj3.value, Map(), base, 1L, false, proj3Id),
                  true)
      .futureValue

  }

  before {
    Mockito.reset(resources)
    Mockito.reset(adminClient)
    when(adminClient.getProjectAcls(anyString, anyString, is(true), is(false))(is(saToken)))
      .thenReturn(Future.successful(
        Some(FullAccessControlList((group, Address("some/path"), Permissions(Permission("resources/manage")))))))
  }

  "A MultiProjectResolution" should {

    "look in all projects to resolve a resource" in {
      val id1 = Id(proj1Id.ref, resId)
      when(resources.fetch(id1, None)).thenReturn(OptionT.none[Future, Resource])

      val id2 = Id(proj2Id.ref, resId)
      when(resources.fetch(id2, None)).thenReturn(OptionT.none[Future, Resource])

      val id3   = Id(proj3Id.ref, resId)
      val value = simpleF(id3, genJson, types = types)
      when(resources.fetch(id3, None)).thenReturn(OptionT.some[Future](value))

      resolution.resolve(Latest(resId)).futureValue.value shouldEqual value
      resolution.resolveAll(Latest(resId)).futureValue shouldEqual List(value)
      verify(resources, times(2)).fetch(id1, None)
      verify(resources, times(2)).fetch(id2, None)
    }

    "short-circuit project traversal when possible" in {
      val id1    = Id(proj1Id.ref, resId)
      val value1 = simpleF(id1, genJson, types = types)
      when(resources.fetch(id1, None)).thenReturn(OptionT.some[Future](value1))

      val id2    = Id(proj2Id.ref, resId)
      val value2 = simpleF(id2, genJson, types = types)
      when(resources.fetch(id2, None)).thenReturn(OptionT.some[Future](value2))

      val id3    = Id(proj3Id.ref, resId)
      val value3 = simpleF(id3, genJson, types = types)
      when(resources.fetch(id3, None)).thenReturn(OptionT.some[Future](value3))

      resolution.resolve(Latest(resId)).futureValue shouldEqual Some(value1)
      verify(resources, times(0)).fetch(id2, None)
      verify(resources, times(0)).fetch(id3, None)
      resolution.resolveAll(Latest(resId)).futureValue shouldEqual List(value1, value2, value3)
    }

    "filter results according to the resolvers' resource types" in {
      val id1    = Id(proj1Id.ref, resId)
      val value1 = simpleF(id1, genJson)
      when(resources.fetch(id1, None)).thenReturn(OptionT.some[Future](value1))

      val id2    = Id(proj2Id.ref, resId)
      val value2 = simpleF(id2, genJson, types = Set(nxv.Schema.value))
      when(resources.fetch(id2, None)).thenReturn(OptionT.some[Future](value2))

      val id3    = Id(proj3Id.ref, resId)
      val value3 = simpleF(id3, genJson, types = Set(nxv.Ontology.value))
      when(resources.fetch(id3, None)).thenReturn(OptionT.some[Future](value3))

      resolution.resolve(Latest(resId)).futureValue shouldEqual Some(value2)
      verify(resources, times(0)).fetch(id3, None)
      resolution.resolveAll(Latest(resId)).futureValue shouldEqual List(value2)
      verify(resources, times(1)).fetch(id3, None)
    }

    "filter results according to the resolvers' identities" in {
      Mockito.reset(adminClient)
      when(adminClient.getProjectAcls(is(proj1.account), is(proj1.value), is(true), is(false))(is(saToken)))
        .thenReturn(Future.successful(Some(FullAccessControlList(
          (GroupRef("ldap2", "bbp-ou-nexus"), Address("some/path"), Permissions(Permission("resources/manage")))))))
      when(adminClient.getProjectAcls(is(proj2.account), is(proj2.value), is(true), is(false))(is(saToken)))
        .thenReturn(Future.successful(Some(FullAccessControlList(
          (UserRef("ldap", "dmontero"), Address("some/path"), Permissions(Permission("resources/read")))))))
      when(adminClient.getProjectAcls(is(proj3.account), is(proj3.value), is(true), is(false))(is(saToken)))
        .thenReturn(Future.successful(None))
      val id1    = Id(proj1Id.ref, resId)
      val value1 = simpleF(id1, genJson, types = types)
      when(resources.fetch(id1, None)).thenReturn(OptionT.some[Future](value1))

      val id2    = Id(proj2Id.ref, resId)
      val value2 = simpleF(id2, genJson, types = types)
      when(resources.fetch(id2, None)).thenReturn(OptionT.some[Future](value2))

      val id3    = Id(proj3Id.ref, resId)
      val value3 = simpleF(id3, genJson, types = types)
      when(resources.fetch(id3, None)).thenReturn(OptionT.some[Future](value3))

      resolution.resolve(Latest(resId)).futureValue shouldEqual Some(value2)
      resolution.resolveAll(Latest(resId)).futureValue shouldEqual List(value2)
    }

    "return none if the resource is not found in any project" in {
      when(resources.fetch(Id(proj1Id.ref, resId), None)).thenReturn(OptionT.none[Future, Resource])
      when(resources.fetch(Id(proj2Id.ref, resId), None)).thenReturn(OptionT.none[Future, Resource])
      when(resources.fetch(Id(proj3Id.ref, resId), None)).thenReturn(OptionT.none[Future, Resource])
      resolution.resolve(Latest(resId)).futureValue shouldEqual None
      resolution.resolveAll(Latest(resId)).futureValue shouldEqual Nil
    }
  }

  implicit class ProjectOps(uuid: String) {
    def ref = ProjectRef(uuid)
  }
}
