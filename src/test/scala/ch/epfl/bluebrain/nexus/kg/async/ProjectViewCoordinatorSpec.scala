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
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectRef}
import ch.epfl.bluebrain.nexus.service.indexer.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
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

    "manage lifecycle of views" in {
      val orgUuid = genUUID
      // format: off
      val project = Project(genIri, "some-project", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
      val project2 = Project(genIri, "some-project2", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
      // format: on
      val view  = SparqlView(project.ref, genIri, genUUID, 1L, deprecated = false)
      val view2 = SparqlView(project.ref, genIri, genUUID, 1L, deprecated = false)
      val view3 = SparqlView(project2.ref, genIri, genUUID, 1L, deprecated = false)

      val counterStart = new AtomicInteger(0)
      val counterStop  = new AtomicInteger(0)

      val probe       = TestProbe()
      val childActor1 = system.actorOf(Props(new DummyActor))
      val childActor2 = system.actorOf(Props(new DummyActor))
      val childActor3 = system.actorOf(Props(new DummyActor))
      probe watch childActor1
      probe watch childActor2
      probe watch childActor3

      val coordinatorProps = Props(
        new ProjectViewCoordinatorActor(viewCache) {
          override def startActor(v: View.SingleView, project: Project): ActorRef = {
            counterStart.incrementAndGet()
            if (v == view) childActor1
            else if (v == view2) childActor2
            else childActor3
          }

          override def onActorStopped(view: View.SingleView, project: Project): Task[Unit] = {
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

      projectCache.replace(project).runToFuture.futureValue
      projectCache.replace(project2).runToFuture.futureValue

      coordinator.start(project).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 0)

      coordinator.start(project2).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 0)

      viewCache.put(view2).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 1)

      viewCache.put(view).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 2)

      viewCache.put(view3).runToFuture.futureValue
      eventually(counterStart.get shouldEqual 3)

      viewCache.put(view2.copy(deprecated = true)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 1)
      eventually(counterStart.get shouldEqual 3)
      probe.expectTerminated(childActor2)

      coordinator.stop(OrganizationRef(orgUuid)).runToFuture.futureValue
      eventually(counterStop.get shouldEqual 1)
      eventually(counterStart.get shouldEqual 3)
      probe.expectTerminated(childActor1)
      probe.expectTerminated(childActor3)
    }
  }

  private class DummyActor extends Actor {
    override def receive: Receive = Actor.ignoringBehavior
  }
}
