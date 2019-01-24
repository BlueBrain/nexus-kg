package ch.epfl.bluebrain.nexus.kg.acls

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.singleton._
import akka.testkit.TestKit
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.commons.test.Randomness._
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Group
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.acls.AclsActor.Stop
import ch.epfl.bluebrain.nexus.kg.acls.AclsOpsSpec._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.IamConfig
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
class AclsOpsSpec
    extends TestKit(ActorSystem("AclsOpsSpec"))
    with TestHelper
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with BeforeAndAfter
    with MockitoSugar {

  private implicit val client: IamClient[Task] = mock[IamClient[Task]]
  private implicit val config: IamConfig =
    IamConfig(url"http://base.com".value, url"http://base.com".value, None, 1 second)
  private implicit val serviceAccountToken: Option[AuthToken] = config.serviceAccountToken
  val acls = AccessControlLists(
    / -> resourceAcls(AccessControlList(Group("myGroup", "myRealm") -> Set(Permission.unsafe("resources/manage")))))

  private implicit val tm: Timeout = Timeout(3 seconds)

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(6 seconds, 300 millis)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).join(Cluster(system).selfAddress)
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  trait Context {
    when(client.acls("*" / "*", ancestors = true, self = false)).thenReturn(Task.pure(acls))
    val aclsOps = new AclsOps(startActor())
  }

  "An AclsOps" should {

    "cache the ACLs" in new Context {
      (0 until 10).foreach { _ =>
        aclsOps.fetch().runToFuture.futureValue shouldEqual acls
      }
    }

    "handle exception" in new Context {
      when(client.acls("*" / "*", ancestors = true, self = false)).thenReturn(Task.raiseError(UnauthorizedAccess))
      whenReady(aclsOps.fetch().runToFuture.failed)(_ shouldEqual UnauthorizedAccess)
    }
  }
}

object AclsOpsSpec {
  private[acls] def startActor(name: String = genString())(implicit
                                                           client: IamClient[Task],
                                                           iamConfig: IamConfig,
                                                           as: ActorSystem): ActorRef = {
    val props = ClusterSingletonManager.props(Props(new AclsActor(client)),
                                              terminationMessage = Stop,
                                              settings = ClusterSingletonManagerSettings(as))
    val singletonManager = as.actorOf(props, name)
    as.actorOf(
      ClusterSingletonProxy.props(singletonManagerPath = singletonManager.path.toStringWithoutAddress,
                                  settings = ClusterSingletonProxySettings(as)),
      name = s"${name}Proxy"
    )
  }
}
