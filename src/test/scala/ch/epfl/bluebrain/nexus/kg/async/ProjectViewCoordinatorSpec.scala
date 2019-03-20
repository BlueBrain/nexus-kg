package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, Props}
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
import ch.epfl.bluebrain.nexus.kg.resources.OrganizationRef
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.sourcing.persistence.ProjectionProgress
import ch.epfl.bluebrain.nexus.sourcing.stream.StreamCoordinator
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito.verify
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}
import org.scalatestplus.mockito.MockitoSugar

class ProjectViewCoordinatorSpec
    extends ActorSystemFixture("ProjectViewCoordinatorSpec", true)
    with TestHelper
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with MockitoSugar {

  private implicit val appConfig = Settings(system).appConfig
  private val projectCache       = ProjectCache[Task]
  private val viewCache          = ViewCache[Task]

  "A ProjectViewCoordinator" should {
    val creator = genIri

    val orgUuid = genUUID
    // format: off
    val project = Project(genIri, "some-project", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val project2 = Project(genIri, "some-project2", "some-org", None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val project2Updated = project2.copy(vocab = genIri, rev = 2L)
    // format: on
    val view = SparqlView(project.ref, genIri, genUUID, 1L, deprecated = false)
    val view2 =
      ElasticSearchView(Json.obj(), Set(genIri), None, true, true, project.ref, genIri, genUUID, 1L, deprecated = false)
    val view2Updated = view2.copy(resourceSchemas = Set(genIri), rev = 2L)
    val view3        = SparqlView(project2.ref, genIri, genUUID, 1L, deprecated = false)

    val counterStart = new AtomicInteger(0)
    val counterStop  = new AtomicInteger(0)

    val coordinator1        = mock[StreamCoordinator[Task, ProjectionProgress]]
    val coordinator2        = mock[StreamCoordinator[Task, ProjectionProgress]]
    val coordinator2Updated = mock[StreamCoordinator[Task, ProjectionProgress]]
    val coordinator3        = mock[StreamCoordinator[Task, ProjectionProgress]]
    val coordinator3Updated = mock[StreamCoordinator[Task, ProjectionProgress]]

    val coordinatorProps = Props(
      new ProjectViewCoordinatorActor(viewCache) {
        override def startCoordinator(v: View.SingleView,
                                      proj: Project,
                                      restartOffset: Boolean): StreamCoordinator[Task, ProjectionProgress] = {
          counterStart.incrementAndGet()
          if (v == view && proj == project) coordinator1
          else if (v == view2 && proj == project) coordinator2
          else if (v == view2Updated && proj == project) coordinator2Updated
          else if (v == view3 && proj == project2) coordinator3
          else if (v == view3 && proj == project2Updated && restartOffset) coordinator3Updated
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
        coordinatorRef)

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

    "stop view when view is removed (deprecated) from the cache" ignore {
      viewCache.put(view.copy(deprecated = true)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 1)
      eventually(counterStart.get shouldEqual 3)
      eventually(verify(coordinator1).stop())
    }

    "stop old view start new view when current view updated" ignore {
      viewCache.put(view2Updated).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 2)
      eventually(counterStart.get shouldEqual 4)
      eventually(verify(coordinator2).stop())
    }

    "do nothing when a view that should not re-trigger indexing gets updated" ignore {
      viewCache.put(view3.copy(rev = 2L)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 2)
      eventually(counterStart.get shouldEqual 4)
    }

    "stop all related views when organization is deprecated" ignore {
      coordinator.stop(OrganizationRef(orgUuid)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 2)
      eventually(counterStart.get shouldEqual 4)
      eventually(verify(coordinator2Updated).stop())
    }

    "restart all related views when project changes" ignore {
      projectCache.replace(project2Updated).runToFuture.futureValue
      coordinator.change(project2Updated, project2).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 3)
      eventually(counterStart.get shouldEqual 5)
      eventually(verify(coordinator3).stop())
    }

    "stop related views when project is deprecated" ignore {
      projectCache.replace(project2Updated.copy(deprecated = true)).runToFuture.futureValue
      coordinator.stop(project2Updated.ref).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 3)
      eventually(counterStart.get shouldEqual 5)
      eventually(verify(coordinator3Updated).stop())
    }
  }

  private class DummyActor extends Actor {
    override def receive: Receive = Actor.ignoringBehavior
  }
}
