package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Props
import akka.testkit.DefaultTimeout
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.commons.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.onViewChange
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticSearchView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, OrganizationRef}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.projections.{ProjectionProgress, Projections, StreamSupervisor}
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class ProjectViewCoordinatorSpec
    extends ActorSystemFixture("ProjectViewCoordinatorSpec", true)
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
  private val viewCache          = ViewCache[Task]

  "A ProjectViewCoordinator" should {
    val creator = genIri

    val orgUuid = genUUID
    // format: off
    val project = Project(genIri, "some-project", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val project2 = Project(genIri, "some-project2", "some-org", None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val project2Updated = project2.copy(label = genString(), rev = 2L)
    // format: on
    val view = SparqlView(Set.empty, Set.empty, None, true, true, project.ref, genIri, genUUID, 1L, deprecated = false)
    val view2 =
      ElasticSearchView(
        Json.obj(),
        Set(genIri),
        Set.empty,
        None,
        true,
        true,
        true,
        project.ref,
        genIri,
        genUUID,
        1L,
        deprecated = false
      )
    val view2Updated = view2.copy(resourceSchemas = Set(genIri), rev = 2L)
    val view3 =
      SparqlView(Set.empty, Set.empty, None, true, true, project2.ref, genIri, genUUID, 1L, deprecated = false)

    val counterStart = new AtomicInteger(0)
    val counterStop  = new AtomicInteger(0)

    val coordinator1         = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator2         = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator2Updated  = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator3         = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator3Updated  = mock[StreamSupervisor[Task, ProjectionProgress]]
    implicit val projections = mock[Projections[Task, Event]]

    val coordinatorProps = Props(
      new ProjectViewCoordinatorActor(viewCache) {
        override def startCoordinator(
            v: View.SingleView,
            proj: Project,
            restartOffset: Boolean
        ): StreamSupervisor[Task, ProjectionProgress] = {
          counterStart.incrementAndGet()
          if (v == view && proj == project) coordinator1
          else if (v == view2 && proj == project) coordinator2
          else if (v == view2Updated && proj == project) coordinator2Updated
          else if (v == view3 && proj == project2) coordinator3
          else if (v == view3.copy(rev = 2L) && proj == project2) coordinator3
          else if (v == view3.copy(rev = 2L) && proj == project2Updated && restartOffset) coordinator3Updated
          else throw new RuntimeException()
        }

        override def deleteViewIndices(view: View.SingleView, project: Project): Task[Unit] = {
          counterStop.incrementAndGet()
          Task.unit
        }

        override def onChange: OnKeyValueStoreChange[AbsoluteIri, View] =
          onViewChange(self)
      }
    )

    val coordinatorRef = ProjectViewCoordinatorActor.start(coordinatorProps, None, 1)
    val coordinator =
      new ProjectViewCoordinator[Task](
        Caches(projectCache, viewCache, mock[ResolverCache[Task]], mock[StorageCache[Task]]),
        coordinatorRef
      )

    projections.progress(any[String]) shouldReturn Task.pure(ProjectionProgress.NoProgress)

    "initialize projects" in {
      projectCache.replace(project).runToFuture.futureValue
      projectCache.replace(project2).runToFuture.futureValue

      coordinator.start(project).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 0)

      coordinator.start(project2).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 0)
    }

    "start view indexer when views are cached" in {
      viewCache.put(view2).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 1)

      viewCache.put(view).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 2)

      viewCache.put(view3).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 3)
    }

    "stop view when view is removed (deprecated) from the cache" in {
      viewCache.put(view.copy(deprecated = true)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 1)
      eventually(counterStart.get shouldEqual 3)
      eventually(coordinator1.stop() wasCalled once)
    }

    "stop old elasticsearch view start new view when current view updated" in {
      viewCache.put(view2Updated).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 2)
      eventually(counterStart.get shouldEqual 4)
      eventually(coordinator2.stop() wasCalled once)
    }

    "stop old sparql view start new view when current view updated" in {
      viewCache.put(view3.copy(rev = 2L)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 3)
      eventually(counterStart.get shouldEqual 5)
      eventually(coordinator3.stop() wasCalled once)
    }

    "stop all related views when organization is deprecated" in {
      coordinator.stop(OrganizationRef(orgUuid)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 3)
      eventually(counterStart.get shouldEqual 5)
      eventually(coordinator2Updated.stop() wasCalled once)
    }

    "restart all related views when project changes" in {
      projectCache.replace(project2Updated).runToFuture.futureValue
      coordinator.change(project2Updated, project2).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 4)
      eventually(counterStart.get shouldEqual 6)
      eventually(coordinator3.stop() wasCalled twice)
    }

    "stop related views when project is deprecated" in {
      projectCache.replace(project2Updated.copy(deprecated = true)).runToFuture.futureValue
      coordinator.stop(project2Updated.ref).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 4)
      eventually(counterStart.get shouldEqual 6)
      eventually(coordinator3Updated.stop() wasCalled once)
    }
  }
}
