package ch.epfl.bluebrain.nexus.kg.acls

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.singleton._
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Address._
import ch.epfl.bluebrain.nexus.iam.client.types.FullAccessControlList
import ch.epfl.bluebrain.nexus.kg.acls.AclsActor.{AclsFetchError, Fetch, Refresh}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.IamConfig
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.Stop
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future
import scala.util.control.NonFatal

/**
  * Actor that caches the ACLs for all identities on all organizations and projects.
  * It refreshes the ACLs after maximum inactivity period of ''iamConfig.cacheTimeout''.
  *
  * @param client the [[IamClient]]
  */
class AclsActor(client: IamClient[Task])(implicit iamConfig: IamConfig) extends Actor with ActorLogging {

  private val taskAcls: Task[Either[Throwable, FullAccessControlList]] = client
    .getAcls("*" / "*", parents = true, self = false)(iamConfig.serviceAccountToken)
    .map(Right.apply)
    .onErrorRecover {
      case NonFatal(th) => Left(th)
    }

  private var acls: Future[Either[Throwable, FullAccessControlList]] = _

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(iamConfig.cacheRefreshInterval, self, Refresh)
    acls = taskAcls.runAsync
  }

  def fetch(requester: ActorRef, retryOnError: Boolean = false): Unit =
    acls.foreach {
      case Right(r) =>
        requester ! r
      case Left(err) =>
        requester ! AclsFetchError(err)
        if (retryOnError)
          acls = taskAcls.runAsync
    }

  override def receive: Receive = {
    case Refresh =>
      acls = taskAcls.runAsync
      fetch(sender(), retryOnError = false)
      val _ = context.system.scheduler.scheduleOnce(iamConfig.cacheRefreshInterval, self, Refresh)
    case Fetch =>
      val _ = fetch(sender())
    // $COVERAGE-OFF$
    case Stop =>
      log.info("Received stop signal, stopping")
      context stop self
    // $COVERAGE-ON$
  }
}

object AclsActor {

  /**
    * Actor messages enumeration
    */
  sealed trait Msg extends Product with Serializable

  /**
    * Fetches the ACLs
    */
  final case object Fetch extends Msg

  /**
    * Forces refreshing ACLs
    */
  final case object Refresh extends Msg

  /**
    * Stops the Actor
    */
  final case object Stop extends Msg

  /**
    * Response message when [[IamClient]] signals an error when fetching ACLs
    * @param err
    */
  final case class AclsFetchError(err: Throwable) extends Msg

  /**
    * Instantiates an actor that maintains the ACLs for a maximum inactivity period of ''iamConfig.cacheTimeout''.
    */
  // $COVERAGE-OFF$
  final def start(implicit client: IamClient[Task], iamConfig: IamConfig, as: ActorSystem): ActorRef = {
    val props = ClusterSingletonManager.props(Props(new AclsActor(client)),
                                              terminationMessage = Stop,
                                              settings = ClusterSingletonManagerSettings(as))
    val singletonManager = as.actorOf(props, "iamAcls")
    as.actorOf(
      ClusterSingletonProxy.props(singletonManagerPath = singletonManager.path.toStringWithoutAddress,
                                  settings = ClusterSingletonProxySettings(as)),
      name = "iamAclsProxy"
    )
  }
  // $COVERAGE-ON$
}
