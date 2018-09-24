package ch.epfl.bluebrain.nexus.kg.acls

import akka.actor.ActorRef
import akka.pattern.{ask, AskTimeoutException}
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.iam.client.types.FullAccessControlList
import ch.epfl.bluebrain.nexus.kg.acls.AclsActor._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{DownstreamServiceError, Unexpected}
import journal.Logger
import monix.eval.Task

import scala.util.control.NonFatal

/**
  * An ACL operations implementation that uses Akka actors to maintain the ACLs state.
  *
  * @param aclsRef a reference to the [[AclsActor]]
  */
class AclsOps(aclsRef: ActorRef)(implicit tm: Timeout) {

  private val log = Logger[this.type]

  /**
    * @return the ACLs for all the identities in all the paths using the provided service account token.
    */
  def fetch(): Task[FullAccessControlList] =
    Task.deferFuture(aclsRef ? Fetch) onErrorRecoverWith {
      // $COVERAGE-OFF$
      case _: AskTimeoutException =>
        log.error("Timed out while fetching acls from IAM service")
        Task.raiseError(DownstreamServiceError("Timed out while fetching acls from IAM service"))
      case NonFatal(th) =>
        log.error("Unexpected exception while fetching acls from IAM service", th)
        Task.raiseError(Unexpected(th.getMessage))
      // $COVERAGE-ON$
    } flatMap {
      case acl: FullAccessControlList => Task.pure(acl)
      case AclsFetchError(err)        => Task.raiseError(err)
      // $COVERAGE-OFF$
      case msg =>
        log.error(s"Received an unexpected message '$msg' while waiting for a 'Fetch' response")
        Task.raiseError(Unexpected("Received an unexpected message while waiting for a 'Fetch' response"))
      // $COVERAGE-ON$
    }
}
