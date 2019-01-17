package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant

import akka.testkit._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.resources.OrganizationRef
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Inspectors, Matchers}

import scala.concurrent.duration._

class ProjectCacheSpec
    extends ActorSystemFixture("ProjectCacheSpec", true)
    with Randomness
    with Matchers
    with Inspectors
    with ScalaFutures
    with TestHelper {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3.seconds.dilated, 5.milliseconds)

  private implicit val appConfig = Settings(system).appConfig

  private val org1      = genUUID
  private val org1Label = genString()
  private val org2      = genUUID
  private val org2Label = genString()

  private val project = Project(genIri,
                                "some-project",
                                "some-org",
                                None,
                                genIri,
                                genIri,
                                Map.empty,
                                genUUID,
                                org1,
                                1L,
                                deprecated = false,
                                Instant.EPOCH,
                                genIri,
                                Instant.EPOCH,
                                genIri)

  val projectsOrg1 = List.fill(10)(
    project
      .copy(id = genIri,
            label = genString(),
            organizationLabel = org1Label,
            organizationUuid = org1,
            base = genIri,
            uuid = genUUID))
  val projectsOrg2 = List.fill(10)(
    project
      .copy(id = genIri,
            label = genString(),
            organizationLabel = org2Label,
            organizationUuid = org2,
            base = genIri,
            uuid = genUUID))

  private val cache = ProjectCache[Task]

  "ProjectCache" should {

    "index projects" in {
      forAll(projectsOrg1 ++ projectsOrg2) { proj =>
        cache.replace(proj).runToFuture.futureValue
        cache.get(proj.ref).runToFuture.futureValue shouldEqual Some(proj)
      }
    }

    "list projects" in {
      cache.list(OrganizationRef(org1)).runToFuture.futureValue should contain theSameElementsAs projectsOrg1
      cache.list(OrganizationRef(org2)).runToFuture.futureValue should contain theSameElementsAs projectsOrg2
    }

    "get project labels" in {
      forAll(projectsOrg1 ++ projectsOrg2) { proj =>
        cache.getLabel(proj.ref).runToFuture.futureValue shouldEqual Some(proj.projectLabel)
      }
    }

    "get project refs to labels map" in {
      val expected = projectsOrg1.map(project => project.ref -> Option(project.projectLabel)).toMap
      cache.getProjectLabels(expected.keySet).runToFuture.futureValue shouldEqual expected
    }

    "get project labels to refs map" in {
      val expected = projectsOrg1.map(project => project.projectLabel -> Option(project.ref)).toMap
      cache.getProjectRefs(expected.keySet).runToFuture.futureValue shouldEqual expected
    }

    "deprecate project" in {
      val project = projectsOrg1.head
      cache.deprecate(project.ref, 2L).runToFuture.futureValue
      cache.get(project.ref).runToFuture.futureValue shouldEqual Some(project.copy(deprecated = true, rev = 2L))
    }
  }
}
