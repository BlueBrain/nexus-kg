package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, TestKit}
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class DistributedCacheSpec
    extends TestKit(ActorSystem("ProjectsSpec"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def genUUID = java.util.UUID.randomUUID.toString

  private val cache = DistributedCache.future()

  private val base = Iri.absolute("https://nexus.example.com").getOrElse(fail)

  "A Projects distributed cache" should {

    "handle accounts life-cycle" in {
      val uuid    = genUUID
      val ref     = AccountRef(uuid)
      val account = Account("some-org", 1L, "some-label", false, uuid)
      cache.addAccount(ref, account, true).futureValue shouldEqual true
      cache.addAccount(ref, account.copy(rev = 2L), false).futureValue shouldEqual false
      cache.account(ref).futureValue shouldEqual Some(account)
      cache.addAccount(ref, account.copy(rev = 3L), true).futureValue shouldEqual true
      cache.account(ref).futureValue shouldEqual Some(account.copy(rev = 3L))
      cache.deprecateAccount(ref, 4L).futureValue shouldEqual true
      cache.account(ref).futureValue shouldEqual Some(account.copy(rev = 4L, deprecated = true))
    }

    "fail when adding a project without adding first its account" in {
      val uuid       = genUUID
      val ref        = ProjectRef(uuid)
      val accountRef = AccountRef(uuid)
      val project    = Project("some-project", "some-label-proj", Map.empty, base, 1L, false, uuid)
      cache.addProject(ref, accountRef, project, Instant.now, true).futureValue shouldEqual false
    }

    "handle projects life-cycle" in {
      val accountUuid = genUUID
      val accountRef  = AccountRef(accountUuid)
      val account     = Account("some-org", 1L, "some-label", false, accountUuid)
      cache.addAccount(accountRef, account, true).futureValue shouldEqual true

      val ref      = ProjectRef(genUUID)
      val ref2     = ProjectRef(genUUID)
      val project  = Project("some-project", "some-label-proj", Map.empty, base, 1L, false, ref.id)
      val project2 = Project("some-project2", "some-label-proj2", Map.empty, base, 42L, false, ref2.id)

      cache.addProject(ref, accountRef, project, Instant.now, false).futureValue shouldEqual true
      cache.addProject(ref, accountRef, project.copy(rev = 2L), Instant.now, false).futureValue shouldEqual false
      cache.project(ref).futureValue shouldEqual Some(project)
      cache.project(ProjectLabel("some-label", "some-label-proj")).futureValue shouldEqual Some(project)
      cache.projects(accountRef).futureValue shouldEqual Set(ref)
      cache.addProject(ref2, accountRef, project2, Instant.now, true).futureValue shouldEqual true
      cache.projects(accountRef).futureValue shouldEqual Set(ref, ref2)
      cache.project(ProjectLabel("wrong", "some-label-proj")).futureValue shouldEqual None
      cache.projectRef(ProjectLabel("some-label", "some-label-proj")).futureValue shouldEqual Some(ref)
      cache.projectRef(ProjectLabel("some-label", "wrong")).futureValue shouldEqual None
      cache.addProject(ref, accountRef, project.copy(rev = 3L), Instant.now, true).futureValue shouldEqual true
      cache.project(ref).futureValue shouldEqual Some(project.copy(rev = 3L))
      cache.deprecateProject(ref, accountRef, Instant.now, 4L).futureValue shouldEqual true
      cache.project(ref).futureValue shouldEqual Some(project.copy(rev = 4L, deprecated = true))
      cache.projects(accountRef).futureValue shouldEqual Set(ref2)
    }

    "handle resolvers life-cycle" in {
      val accountUuid = genUUID
      val accountRef  = AccountRef(accountUuid)
      val account     = Account("some-org", 1L, "some-label", false, accountUuid)
      cache.addAccount(accountRef, account, true).futureValue shouldEqual true

      val projectUuid = genUUID
      val ref         = ProjectRef(projectUuid)
      val project     = Project("some-project", "some-label-proj", Map.empty, base, 1L, false, projectUuid)

      cache.addProject(ref, accountRef, project, Instant.now, true).futureValue shouldEqual true

      val projectId = base + "some-label/some-label-proj"
      val resolver  = InProjectResolver(ref, projectId, 1L, false, 10)

      val resolver2 = CrossProjectResolver(Set(nxv.Schema.value),
                                           Set(ProjectRef(genUUID)),
                                           List(Anonymous),
                                           ref,
                                           url"http://cross-project-resolver.com".value,
                                           1L,
                                           false,
                                           1)

      cache.addResolver(ref, resolver).futureValue shouldEqual true
      cache.resolvers(ref).futureValue shouldEqual Set(resolver)

      cache.applyResolver(ref, resolver.copy(rev = 2L)).futureValue shouldEqual true
      cache.resolvers(ref).futureValue shouldEqual Set(resolver.copy(rev = 2L))

      cache.addResolver(ref, resolver2).futureValue shouldEqual true
      cache.resolvers(ref).futureValue shouldEqual Set(resolver.copy(rev = 2L), resolver2)

      cache.addResolver(ref, resolver.copy(rev = 1L)).futureValue shouldEqual true
      cache.resolvers(ref).futureValue shouldEqual Set(resolver.copy(rev = 2L), resolver2)

      cache.removeResolver(ref, projectId, 3L).futureValue shouldEqual true
      cache.resolvers(ref).futureValue shouldEqual Set(resolver2)
    }

    "handle views life-cycle" in {
      val accountUuid = genUUID
      val accountRef  = AccountRef(accountUuid)
      val account     = Account("some-org", 1L, "some-label", false, accountUuid)
      cache.addAccount(accountRef, account, true).futureValue shouldEqual true

      val projectUuid = genUUID
      val ref         = ProjectRef(projectUuid)
      val project     = Project("some-project", "some-label-proj", Map.empty, base, 1L, false, projectUuid)

      cache.addProject(ref, accountRef, project, Instant.now, true).futureValue shouldEqual true

      val projectId = base + "some-label/some-label-proj"
      val view      = ElasticView(ref, projectId, genUUID, 1L, false)
      cache.addView(ref, view, Instant.now, true).futureValue shouldEqual true
      cache.views(ref).futureValue shouldEqual Set(view)
      cache.views(ProjectLabel("some-label", "some-label-proj")).futureValue shouldEqual Set(view)
      cache.views(ProjectLabel("wrong", "some-label-proj")).futureValue shouldEqual Set.empty[View]
      cache.applyView(ref, view.copy(rev = 2L), Instant.now).futureValue shouldEqual true
      cache.views(ref).futureValue shouldEqual Set(view, view.copy(rev = 2L))
      cache.addView(ref, view.copy(rev = 3L), Instant.now, false).futureValue shouldEqual false
      cache.views(ref).futureValue shouldEqual Set(view, view.copy(rev = 2L))
      cache.removeView(ref, projectId, Instant.now).futureValue shouldEqual true
      cache.views(ref).futureValue shouldEqual Set.empty
    }
  }
}
