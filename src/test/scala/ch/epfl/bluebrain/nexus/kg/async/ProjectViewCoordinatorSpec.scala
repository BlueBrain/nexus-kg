package ch.epfl.bluebrain.nexus.kg.async

import java.time.{Clock, Instant}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, Props}
import akka.testkit.{DefaultTimeout, TestProbe}
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.onViewChange
import ch.epfl.bluebrain.nexus.kg.async.ViewCache.RevisionedViews
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectRef}
import ch.epfl.bluebrain.nexus.service.indexer.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpecLike}

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
  private implicit val clock     = Clock.systemUTC
  private val projectCache       = ProjectCache[Task]
  private val viewCache          = ViewCache[Task]

  "A ProjectViewCoordinator" should {
    val creator = genIri

    val orgUuid = genUUID
    // format: off
    val project = Project(genIri, "some-project", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val project2 = Project(genIri, "some-project2", "some-org", None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val project2Updated = project2.copy(vocab = genIri)
    // format: on
    val view = SparqlView(project.ref, genIri, genUUID, 1L, deprecated = false)
    val view2 =
      ElasticView(Json.obj(), Set(genIri), None, true, true, project.ref, genIri, genUUID, 1L, deprecated = false)
    val view2Updated = view2.copy(resourceSchemas = Set(genIri))
    val view3        = SparqlView(project2.ref, genIri, genUUID, 1L, deprecated = false)

    val counterStart = new AtomicInteger(0)
    val counterStop  = new AtomicInteger(0)

    val probe              = TestProbe()
    val childActor1        = system.actorOf(Props(new DummyActor))
    val childActor2        = system.actorOf(Props(new DummyActor))
    val childActor2Updated = system.actorOf(Props(new DummyActor))
    val childActor3        = system.actorOf(Props(new DummyActor))
    val childActor3Updated = system.actorOf(Props(new DummyActor))

    probe watch childActor1
    probe watch childActor2
    probe watch childActor2Updated
    probe watch childActor3
    probe watch childActor3Updated

    val coordinatorProps = Props(
      new ProjectViewCoordinatorActor(viewCache) {
        override def startActor(v: View.SingleView, proj: Project): ActorRef = {
          counterStart.incrementAndGet()
          if (v == view && proj == project) childActor1
          else if (v == view2 && proj == project) childActor2
          else if (v == view2Updated && proj == project) childActor2Updated
          else if (v == view3 && proj == project2) childActor3
          else if (v == view3 && proj == project2Updated) childActor3Updated
          else system.actorOf(Props(new DummyActor))
        }

        override def deleteViewIndices(view: View.SingleView, project: Project): Task[Unit] = {
          counterStop.incrementAndGet()
          Task.unit
        }

        override def onChange(projectRef: ProjectRef): OnKeyValueStoreChange[UUID, RevisionedViews] =
          onViewChange(projectRef, self)
      }
    )

    val coordinatorRef = ProjectViewCoordinatorActor.start(coordinatorProps, None, 1)
    val coordinator =
      new ProjectViewCoordinator[Task](Caches(projectCache, viewCache, mock[ResolverCache[Task]]), coordinatorRef)

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
      probe.expectTerminated(childActor1)
    }

    "stop old view start new view when current view updated" in {
      viewCache.put(view2Updated).runToFuture.futureValue
      probe.expectTerminated(childActor2)
      eventually(counterStop.get shouldEqual 2)
      eventually(counterStart.get shouldEqual 4)
    }

    "do nothing when a view that should not re-trigger indexing gets updated" in {
      viewCache.put(view3.copy(rev = 2L)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 2)
      eventually(counterStart.get shouldEqual 4)
    }

    "stop all related views when organization is deprecated" in {
      coordinator.stop(OrganizationRef(orgUuid)).runToFuture.futureValue
      probe.expectTerminated(childActor2Updated)
      eventually(counterStop.get shouldEqual 2)
      eventually(counterStart.get shouldEqual 4)
    }

    "restart all related views when project changes" in {
      projectCache.replace(project2Updated).runToFuture.futureValue
      coordinator.change(project2Updated, project2).runToFuture.futureValue
      probe.expectTerminated(childActor3)
      eventually(counterStop.get shouldEqual 3)
      eventually(counterStart.get shouldEqual 5)
    }

    "stop related views when project is deprecated" in {
      projectCache.replace(project2Updated.copy(deprecated = true)).runToFuture.futureValue
      coordinator.stop(project2Updated.ref).runToFuture.futureValue
      probe.expectTerminated(childActor3Updated)
      eventually(counterStop.get shouldEqual 3)
      eventually(counterStart.get shouldEqual 5)
    }
  }

  private class DummyActor extends Actor {
    override def receive: Receive = Actor.ignoringBehavior
  }
}
