package ch.epfl.bluebrain.nexus.kg.async

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.testkit.{DefaultTimeout, TestKit, TestProbe}
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator.Msg
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.SparqlView
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
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

  private val base = url"https://nexus.example.com".value

  private val cache = DistributedCache.task()

  "A ProjectViewCoordinator" should {
    "create and kill child actors when views change" in {
      val projUUID       = genUUID
      val accUUID        = genUUID
      val viewUUID       = genUUID
      val projectRef     = ProjectRef(projUUID)
      val accountRef     = AccountRef(accUUID)
      val project        = Project("some-project", "some-label-proj", Map.empty, base, 1L, deprecated = false, projUUID)
      val account        = Account("some-org", 1L, "some-label", deprecated = false, accUUID)
      val viewId         = base + "projects/some-project/search"
      val view           = SparqlView(projectRef, viewId, viewUUID, 1L, deprecated = false)
      val counter        = new AtomicInteger(0)
      val childActor     = system.actorOf(Props(new DummyActor))
      val probe          = TestProbe()
      val labeledProject = LabeledProject(ProjectLabel(account.label, project.label), project, accountRef)

      probe watch childActor

      def selector(view: View, lp: LabeledProject): ActorRef = view match {
        case v: SparqlView =>
          if (lp == labeledProject)
            v shouldEqual view
          counter.incrementAndGet()
          childActor
        case _ => fail()
      }

      val coordinator = ProjectViewCoordinator.start(cache, selector, None, 1)
      cache.addAccount(accountRef, account, updateRev = true).runAsync.futureValue shouldEqual true
      cache.addProject(projectRef, accountRef, project, updateRev = true).runAsync.futureValue shouldEqual true
      coordinator ! Msg(accountRef, projectRef)
      cache.addView(projectRef, view, updateRev = true).runAsync.futureValue shouldEqual true
      eventually { counter.get shouldEqual 1 }
      cache.removeView(projectRef, viewId, 2L).runAsync.futureValue shouldEqual true
      probe.expectTerminated(childActor)
    }
  }

  private class DummyActor extends Actor {
    override def receive: Receive = Actor.ignoringBehavior
  }
}
