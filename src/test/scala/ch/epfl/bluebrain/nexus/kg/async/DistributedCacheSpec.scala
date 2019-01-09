package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant

import akka.actor.ActorSystem
import akka.testkit.{DefaultTimeout, TestKit}
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class DistributedCacheSpec
    extends TestKit(ActorSystem("ProjectsSpec"))
    with TestHelper
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with Randomness {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(6 seconds, 100 millis)

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private val cache = DistributedCache.future()

  private val base = Iri.absolute("https://nexus.example.com").getOrElse(fail)

  "A Projects distributed cache" should {

    val creator = genIri

    sealed trait Context {
      val orgUuid  = genUUID
      val orgRef   = OrganizationRef(orgUuid)
      val orgLabel = genString(length = 4)
      val org = Organization(genIri,
                             orgLabel,
                             genString(),
                             orgUuid,
                             1L,
                             deprecated = false,
                             Instant.EPOCH,
                             creator,
                             Instant.EPOCH,
                             creator)

      val projectUuid  = genUUID
      val projectRef   = ProjectRef(projectUuid)
      val projectLabel = genString(length = 4)
      val project = Project(genIri,
                            projectLabel,
                            orgLabel,
                            None,
                            base,
                            Map.empty,
                            projectUuid,
                            1L,
                            deprecated = false,
                            Instant.EPOCH,
                            creator,
                            Instant.EPOCH,
                            creator)
    }

    "handle organizations life-cycle" in new Context {
      cache.addOrganization(orgRef, org).futureValue shouldEqual (())
      cache.organization(orgRef).futureValue shouldEqual Some(org)
      cache.addOrganization(orgRef, org.copy(rev = 3L)).futureValue shouldEqual (())
      cache.organization(orgRef).futureValue shouldEqual Some(org.copy(rev = 3L))
      cache.deprecateOrganization(orgRef, 4L).futureValue shouldEqual (())
      cache.organization(orgRef).futureValue shouldEqual Some(org.copy(rev = 4L, deprecated = true))
    }

    "fail when adding a project without adding first its organization" in new Context {
      cache.addProject(projectRef, orgRef, project).failed.mapTo[RetriableErr].futureValue
    }

    "handle projects life-cycle" in new Context {
      cache.addOrganization(orgRef, org).futureValue shouldEqual (())

      val ref2 = ProjectRef(genUUID)
      val project2 = Project(genIri,
                             "some-project2",
                             "some-label-proj2",
                             None,
                             base,
                             Map.empty,
                             ref2.id,
                             42L,
                             deprecated = false,
                             Instant.EPOCH,
                             creator,
                             Instant.EPOCH,
                             creator)

      cache.addProject(projectRef, orgRef, project).futureValue shouldEqual (())
      cache.project(projectRef).futureValue shouldEqual Some(project)
      cache.project(ProjectLabel(orgLabel, projectLabel)).futureValue shouldEqual Some(project)
      cache.projects(orgRef).futureValue shouldEqual Set(projectRef)
      cache.addProject(ref2, orgRef, project2).futureValue shouldEqual (())
      cache.projects(orgRef).futureValue shouldEqual Set(projectRef, ref2)
      cache.project(ProjectLabel("wrong", "some-label-proj")).futureValue shouldEqual None
      cache.projectRef(ProjectLabel(orgLabel, projectLabel)).futureValue shouldEqual Some(projectRef)
      cache.projectRef(ProjectLabel(orgLabel, "wrong")).futureValue shouldEqual None
      cache
        .addProject(projectRef, orgRef, project.copy(rev = 3L))
        .futureValue shouldEqual (())
      cache.project(projectRef).futureValue shouldEqual Some(project.copy(rev = 3L))
      cache.deprecateProject(projectRef, orgRef, 4L).futureValue shouldEqual (())
      cache.project(projectRef).futureValue shouldEqual Some(project.copy(rev = 4L, deprecated = true))
      cache.projects(orgRef).futureValue shouldEqual Set(ref2)
    }

    "handle resolvers life-cycle" in new Context {
      cache.addOrganization(orgRef, org).futureValue shouldEqual (())
      cache.addProject(projectRef, orgRef, project).futureValue shouldEqual (())

      val projectId = base + s"$orgLabel/$projectLabel"
      val resolver  = InProjectResolver(projectRef, projectId, 1L, deprecated = false, 10)

      val resolver2 = CrossProjectResolver(Set(nxv.Schema.value),
                                           Set(ProjectRef(genUUID)),
                                           List(Anonymous),
                                           projectRef,
                                           url"http://cross-project-resolver.com".value,
                                           1L,
                                           deprecated = false,
                                           1)

      cache.addResolver(projectRef, resolver).futureValue shouldEqual (())
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver)

      cache.applyResolver(projectRef, resolver.copy(rev = 2L)).futureValue shouldEqual (())
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver.copy(rev = 2L))

      cache.addResolver(projectRef, resolver2).futureValue shouldEqual (())
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver.copy(rev = 2L), resolver2)

      cache.addResolver(projectRef, resolver.copy(rev = 1L)).futureValue shouldEqual (())
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver.copy(rev = 2L), resolver2)

      cache.removeResolver(projectRef, projectId, 3L).futureValue shouldEqual (())
      cache.resolvers(projectRef).futureValue shouldEqual Set(resolver2)
    }

    "handle views life-cycle" in new Context {
      cache.addOrganization(orgRef, org).futureValue shouldEqual (())

      cache.addProject(projectRef, orgRef, project).futureValue shouldEqual (())

      val projectId = base + s"$orgLabel/$projectLabel"
      val view      = SparqlView(projectRef, projectId, genUUID, 1L, deprecated = false)
      val view2     = SparqlView(projectRef, url"http://some.project/id".value, genUUID, 1L, deprecated = false)
      cache.addView(projectRef, view).futureValue shouldEqual (())
      cache.views(projectRef).futureValue shouldEqual Set(view)
      cache.views(ProjectLabel(orgLabel, projectLabel)).futureValue shouldEqual Set(view)
      cache.views(ProjectLabel("wrong", "some-label-proj")).futureValue shouldEqual Set.empty[View]
      cache.applyView(projectRef, view.copy(rev = 2L)).futureValue shouldEqual (())
      cache.views(projectRef).futureValue shouldEqual Set(view.copy(rev = 2L))
      cache.addView(projectRef, view2).futureValue shouldEqual (())
      cache.views(projectRef).futureValue shouldEqual Set(view.copy(rev = 2L), view2)
      cache.removeView(projectRef, projectId, 2L).futureValue shouldEqual (())
      cache.views(projectRef).futureValue shouldEqual Set(view.copy(rev = 2L), view2)
      cache.removeView(projectRef, projectId, 3L).futureValue shouldEqual (())
      cache.views(projectRef).futureValue shouldEqual Set(view2)
    }
  }
}
