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
import com.github.ghik.silencer.silent
import monix.eval.Task
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

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(30 seconds, 3 seconds)

  private val cluster = Cluster(system)

  override protected def beforeAll(): Unit = cluster.join(cluster.selfAddress)

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def genUUID = java.util.UUID.randomUUID.toString
  private val cache   = DistributedCache.task()

  "A ProjectViewCoordinator" should {
    "manage lifecycle of views" in {
      val base           = url"https://nexus.example.com/$genUUID".value
      val projUUID       = genUUID
      val accUUID        = genUUID
      val viewUUID       = genUUID
      val viewUUID2      = genUUID
      val projectRef     = ProjectRef(projUUID)
      val accountRef     = AccountRef(accUUID)
      val project        = Project("some-project", "some-label-proj", Map.empty, base, 1L, deprecated = false, projUUID)
      val account        = Account("some-org", 1L, "some-label", deprecated = false, accUUID)
      val viewId         = base + "projects/some-project/search"
      val viewId2        = base + "projects/some-project2/search"
      val view           = SparqlView(projectRef, viewId, viewUUID, 1L, deprecated = false)
      val view2          = SparqlView(projectRef, viewId2, viewUUID2, 1L, deprecated = false)
      val counter        = new AtomicInteger(0)
      val counterStop    = new AtomicInteger(0)
      val childActor     = system.actorOf(Props(new DummyActor))
      val probe          = TestProbe()
      val labeledProject = LabeledProject(ProjectLabel(account.label, project.label), project, accountRef)

      def selector(view: View, lp: LabeledProject): ActorRef = view match {
        case v: SparqlView =>
          if (lp == labeledProject)
            v shouldEqual view
          counter.incrementAndGet()
          childActor
        case _ => fail()
      }

      def onStop(@silent view: View): Task[Boolean] = {
        val _ = counterStop.incrementAndGet()
        Task.pure(true)
      }

      probe watch childActor
      val coordinator = ProjectViewCoordinator.start(cache, selector, onStop, None, 1)
      cache.addAccount(accountRef, account).runAsync.futureValue shouldEqual (())
      cache.addProject(projectRef, accountRef, project).runAsync.futureValue shouldEqual (())
      coordinator ! Msg(accountRef, projectRef)
      cache.addView(projectRef, view).runAsync.futureValue shouldEqual (())
      eventually(counter.get shouldEqual 1)
      cache.removeView(projectRef, viewId, 2L).runAsync.futureValue shouldEqual (())
      eventually(counterStop.get shouldEqual 1)

      cache.addView(projectRef, view2).runAsync.futureValue shouldEqual (())
      eventually(counter.get shouldEqual 2)
      cache
        .addProject(projectRef, accountRef, project.copy(deprecated = true, rev = 2L))
        .runAsync
        .futureValue shouldEqual (())
      eventually(counterStop.get shouldEqual 2)
      probe.expectTerminated(childActor)
    }
  }

  private class DummyActor extends Actor {
    override def receive: Receive = Actor.ignoringBehavior
  }
}
