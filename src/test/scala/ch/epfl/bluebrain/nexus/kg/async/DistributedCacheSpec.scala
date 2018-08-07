package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, TestKit}
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
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
    with BeforeAndAfterAll
    with Randomness {

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def genUUID = java.util.UUID.randomUUID.toString

  private val cache = DistributedCache.future()

  private val base = Iri.absolute("https://nexus.example.com").getOrElse(fail)

  "A Projects distributed cache" should {

    sealed trait Context {
      val accountUuid  = genUUID
      val accountRef   = AccountRef(accountUuid)
      val accountLabel = genString(length = 4)
      val account      = Account(genString(), 1L, accountLabel, deprecated = false, accountUuid)

      val projectUuid  = genUUID
      val projectRef   = ProjectRef(projectUuid)
      val projectLabel = genString(length = 4)
      val project      = Project(genString(), projectLabel, Map.empty, base, 1L, deprecated = false, projectUuid)
    }

    "handle accounts life-cycle" in new Context {
      cache.addAccount(accountRef, account, updateRev = true).futureValue shouldEqual true
      cache.addAccount(accountRef, account.copy(rev = 2L), updateRev = false).futureValue shouldEqual false
      cache.account(accountRef).futureValue shouldEqual Some(account)
      cache.addAccount(accountRef, account.copy(rev = 3L), updateRev = true).futureValue shouldEqual true
      cache.account(accountRef).futureValue shouldEqual Some(account.copy(rev = 3L))
      cache.deprecateAccount(accountRef, 4L).futureValue shouldEqual true
      cache.account(accountRef).futureValue shouldEqual Some(account.copy(rev = 4L, deprecated = true))
    }

    "fail when adding a project without adding first its account" in new Context {
      cache.addProject(projectRef, accountRef, project, updateRev = true).futureValue shouldEqual false
    }

    "handle projects life-cycle" in new Context {
      cache.addAccount(accountRef, account, updateRev = true).futureValue shouldEqual true

      val ref2     = ProjectRef(genUUID)
      val project2 = Project("some-project2", "some-label-proj2", Map.empty, base, 42L, deprecated = false, ref2.id)

      cache.addProject(projectRef, accountRef, project, updateRev = false).futureValue shouldEqual true
      cache
        .addProject(projectRef, accountRef, project.copy(rev = 2L), updateRev = false)
        .futureValue shouldEqual false
      cache.project(projectRef).futureValue shouldEqual Some(project)
      cache.project(ProjectLabel(accountLabel, projectLabel)).futureValue shouldEqual Some(project)
      cache.projects(accountRef).futureValue shouldEqual Set(projectRef)
      cache.addProject(ref2, accountRef, project2, updateRev = true).futureValue shouldEqual true
      cache.projects(accountRef).futureValue shouldEqual Set(projectRef, ref2)
      cache.project(ProjectLabel("wrong", "some-label-proj")).futureValue shouldEqual None
      cache.projectRef(ProjectLabel(accountLabel, projectLabel)).futureValue shouldEqual Some(projectRef)
      cache.projectRef(ProjectLabel(accountLabel, "wrong")).futureValue shouldEqual None
      cache
        .addProject(projectRef, accountRef, project.copy(rev = 3L), updateRev = true)
        .futureValue shouldEqual true
      cache.project(projectRef).futureValue shouldEqual Some(project.copy(rev = 3L))
      cache.deprecateProject(projectRef, accountRef, 4L).futureValue shouldEqual true
      cache.project(projectRef).futureValue shouldEqual Some(project.copy(rev = 4L, deprecated = true))
      cache.projects(accountRef).futureValue shouldEqual Set(ref2)
    }

    "handle resolvers life-cycle" in new Context {
      cache.addAccount(accountRef, account, updateRev = true).futureValue shouldEqual true
      cache.addProject(projectRef, accountRef, project, updateRev = true).futureValue shouldEqual true

      val projectId = base + s"$accountLabel/$projectLabel"
      val resolver  = InProjectResolver(projectRef, projectId, 1L, deprecated = false, 10)

      val resolver2 = CrossProjectResolver(Set(nxv.Schema.value),
                                           Set(ProjectRef(genUUID)),
                                           List(Anonymous),
                                           projectRef,
                                           url"http://cross-project-resolver.com".value,
                                           1L,
                                           deprecated = false,
                                           1)

      cache.addResolver(projectRef, resolver).futureValue shouldEqual true
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver)

      cache.applyResolver(projectRef, resolver.copy(rev = 2L)).futureValue shouldEqual true
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver.copy(rev = 2L))

      cache.addResolver(projectRef, resolver2).futureValue shouldEqual true
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver.copy(rev = 2L), resolver2)

      cache.addResolver(projectRef, resolver.copy(rev = 1L)).futureValue shouldEqual true
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver.copy(rev = 2L), resolver2)

      cache.removeResolver(projectRef, projectId, 3L).futureValue shouldEqual true
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver2)
    }

    "handle views life-cycle" in new Context {
      cache.addAccount(accountRef, account, updateRev = true).futureValue shouldEqual true

      cache.addProject(projectRef, accountRef, project, updateRev = true).futureValue shouldEqual true

      val projectId = base + s"$accountLabel/$projectLabel"
      val view      = SparqlView(projectRef, projectId, genUUID, 1L, deprecated = false)
      val view2     = SparqlView(projectRef, url"http://some.project/id".value, genUUID, 1L, deprecated = false)
      cache.addView(projectRef, view, updateRev = true).futureValue shouldEqual true
      cache.views(projectRef).futureValue shouldEqual Set(view)
      cache.views(ProjectLabel(accountLabel, projectLabel)).futureValue shouldEqual Set(view)
      cache.views(ProjectLabel("wrong", "some-label-proj")).futureValue shouldEqual Set.empty[View]
      cache.applyView(projectRef, view.copy(rev = 2L)).futureValue shouldEqual true
      cache.views(projectRef).futureValue shouldEqual Set(view.copy(rev = 2L))
      cache.addView(projectRef, view2, updateRev = false).futureValue shouldEqual true
      cache.views(projectRef).futureValue shouldEqual Set(view.copy(rev = 2L), view2)
      cache.addView(projectRef, view.copy(rev = 3L), updateRev = false).futureValue shouldEqual false
      cache.views(projectRef).futureValue shouldEqual Set(view.copy(rev = 2L), view2)
      cache.removeView(projectRef, projectId, 2L).futureValue shouldEqual true
      cache.views(projectRef).futureValue shouldEqual Set(view.copy(rev = 2L), view2)
      cache.removeView(projectRef, projectId, 3L).futureValue shouldEqual true
      cache.views(projectRef).futureValue shouldEqual Set(view2)
    }
  }
}
