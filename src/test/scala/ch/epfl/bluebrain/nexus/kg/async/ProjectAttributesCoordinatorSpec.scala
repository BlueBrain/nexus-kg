package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant

import akka.persistence.query.Sequence
import akka.testkit.DefaultTimeout
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.ProjectAttributesCoordinator.SupervisorFactory
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.indexing.Statistics
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress.OffsetProgress
import ch.epfl.bluebrain.nexus.sourcing.projections.{ProjectionProgress, StreamSupervisor}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.matchers.MacroBasedMatchers
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito, Mockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.reflect.ClassTag

class ProjectAttributesCoordinatorSpec
    extends ActorSystemFixture("ProjectAttributesCoordinatorSpec", true)
    with TestHelper
    with DefaultTimeout
    with AnyWordSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with MacroBasedMatchers
    with BeforeAndAfter {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(15.second, 150.milliseconds)

  private implicit val appConfig    = Settings(system).appConfig
  private implicit val projectCache = mock[ProjectCache[Task]]
  private val supervisorFactory     = mock[SupervisorFactory[Task]]

  trait Ctx {
    val supervisor: StreamSupervisor[Task, ProjectionProgress] = mock[StreamSupervisor[Task, ProjectionProgress]]
    val progressSupervisor: ProgressStreamSupervisor[Task]     = ProgressStreamSupervisor(genString(), supervisor)
    val creator                                                = genIri
    val orgUuid                                                = genUUID
    // format: off
    val project = Project(genIri, "some-project", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    // format: on
  }

  before(Mockito.reset(supervisorFactory, projectCache))

  "A ProjectAttributesCoordinator" should {

    val coordinator = ProjectAttributesCoordinator(supervisorFactory).runToFuture.futureValue

    "start the progress stream supervisor" in new Ctx {
      supervisorFactory(project) shouldReturn progressSupervisor
      coordinator.start(project).runToFuture.futureValue
      supervisorFactory(project) wasCalled once
    }

    "fetch statistics" in new Ctx {
      supervisorFactory(project) shouldReturn progressSupervisor
      coordinator.start(project).runToFuture.futureValue

      val progress = OffsetProgress(Sequence(genInt().toLong), genInt().toLong, genInt().toLong, genInt().toLong)
      supervisor.state()(any[ClassTag[Option[ProjectionProgress]]]) shouldReturn Task(Some(progress))
      coordinator.statistics(project).runToFuture.futureValue shouldEqual
        Statistics(progress.processed, progress.discarded, progress.failed, progress.processed, None, None)
      supervisor.state()(any[ClassTag[Option[ProjectionProgress]]]) wasCalled once
    }

    "fetch empty statistics" in new Ctx {
      coordinator.statistics(project).runToFuture.futureValue shouldEqual Statistics.empty
    }

    "stop the progress stream supervisor" in new Ctx {
      supervisorFactory(project) shouldReturn progressSupervisor
      coordinator.start(project).runToFuture.futureValue
      supervisor.stop() shouldReturn Task.unit
      coordinator.stop(project.ref).runToFuture.futureValue
      supervisor.stop() wasCalled once
    }

    "stop a non existing progress stream" in new Ctx {
      coordinator.stop(project.ref).runToFuture.futureValue
      supervisor.stop() wasNever called
    }
  }
}
