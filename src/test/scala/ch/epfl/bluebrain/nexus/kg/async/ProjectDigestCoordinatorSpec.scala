package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Props
import akka.testkit.DefaultTimeout
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.resources.{Event, OrganizationRef}
import ch.epfl.bluebrain.nexus.sourcing.projections.{ProjectionProgress, Projections, StreamSupervisor}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class ProjectDigestCoordinatorSpec
    extends ActorSystemFixture("ProjectDigestCoordinatorSpec", true)
    with TestHelper
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with IdiomaticMockito
    with ArgumentMatchersSugar {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(15 second, 150 milliseconds)

  private implicit val appConfig = Settings(system).appConfig
  private val projectCache       = ProjectCache[Task]

  "A ProjectDigestCoordinator" should {
    val creator = genIri

    val orgUuid = genUUID
    // format: off
    val project = Project(genIri, "some-project", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val project2 = Project(genIri, "some-project2", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    // format: on

    val counterStart = new AtomicInteger(0)

    val coordinator1         = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator2         = mock[StreamSupervisor[Task, ProjectionProgress]]
    implicit val projections = mock[Projections[Task, Event]]

    val coordinatorProps = Props(
      new ProjectDigestCoordinatorActor {
        override def startCoordinator(
            proj: Project,
            restartOffset: Boolean
        ): StreamSupervisor[Task, ProjectionProgress] = {
          counterStart.incrementAndGet()
          if (proj == project) coordinator1
          else if (proj == project2) coordinator2
          else throw new RuntimeException()
        }
      }
    )

    val coordinatorRef = ProjectDigestCoordinatorActor.start(coordinatorProps, None, 1)
    val coordinator    = new ProjectDigestCoordinator[Task](projectCache, coordinatorRef)

    projections.progress(any[String]) shouldReturn Task.pure(ProjectionProgress.NoProgress)

    "start digest computation on initialize projects" in {
      projectCache.replace(project).runToFuture.futureValue
      projectCache.replace(project2).runToFuture.futureValue

      coordinator.start(project).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 1)

      coordinator.start(project2).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 2)
    }

    "ignore attempt to start again a digest computation" in {
      coordinator.start(project).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 2)

      coordinator.start(project2).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 2)
    }

    "stop all related digest computations when organization is deprecated" in {
      coordinator1.stop() shouldReturn Task.unit
      coordinator2.stop() shouldReturn Task.unit
      coordinator.stop(OrganizationRef(orgUuid)).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 2)
      eventually(coordinator1.stop() wasCalled once)
      eventually(coordinator2.stop() wasCalled once)
    }

    "restart digest computations" in {
      coordinator.start(project).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 3)

      coordinator.start(project2).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 4)
    }
  }
}
