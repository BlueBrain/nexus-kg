package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.testkit.{DefaultTimeout, TestKit, TestProbe}
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator.Msg
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class ProjectViewCoordinatorSpec
    extends TestKit(ActorSystem("ProjectViewCoordinatorSpec"))
    with DefaultTimeout
    with WordSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with BeforeAndAfterAll {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 3.seconds)

  private val cluster = Cluster(system)

  override protected def beforeAll(): Unit = cluster.join(cluster.selfAddress)

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def genUUID = java.util.UUID.randomUUID.toString

  private val base = Iri.absolute("https://nexus.example.com").getOrElse(fail)

  private val projects = Projects.task()

  "A ProjectViewCoordinator" should {
    "create and kill child actors when views change" in {
      val projUUID   = genUUID
      val accUUID    = genUUID
      val viewUUID   = genUUID
      val projectRef = ProjectRef(projUUID)
      val accountRef = AccountRef(accUUID)
      val project    = Project("some-project", Map.empty, base, 1L, false, projUUID)
      val account    = Account("some-org", 1L, "some-label", false, accUUID)
      val viewId     = base + "projects/some-project/search"
      val view       = ElasticView(projectRef, viewId, viewUUID, 1L, false)
      val counter    = new AtomicInteger(0)
      val childActor = system.actorOf(Props(new DummyActor))
      val probe      = TestProbe()
      probe watch childActor

      def selector(view: View): ActorRef = view match {
        case v: ElasticView =>
          v shouldEqual view
          counter.incrementAndGet()
          childActor
        case _ => fail()
      }

      val coordinator = ProjectViewCoordinator.start(projects, selector, None, 1)
      projects.addAccount(accountRef, account, true).runAsync.futureValue shouldEqual true
      projects.addProject(projectRef, project, true).runAsync.futureValue shouldEqual true
      coordinator ! Msg(accountRef, projectRef)
      projects.addView(projectRef, view, Instant.now, true).runAsync.futureValue shouldEqual true
      eventually { counter.get shouldEqual 1 }
      projects.removeView(projectRef, viewId, Instant.now).runAsync.futureValue shouldEqual true
      probe.expectTerminated(childActor)
    }
  }

  private class DummyActor extends Actor {
    override def receive: Receive = Actor.ignoringBehavior
  }
}
