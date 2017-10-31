package ch.epfl.bluebrain.nexus.kg.core

import java.time.Clock

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.{AuthenticatedRef, UserRef}
import ch.epfl.bluebrain.nexus.commons.iam.identity.{Caller, Identity}

/**
  * Context information for any operation bundle call
  *
  * @param clock  the clock used to issue instants
  * @param caller the author of the operation call
  */
final case class CallerCtx(clock: Clock, caller: Caller) {
  lazy val meta = Meta(identity(), clock.instant())

  private def identity(): Identity =
    caller.identities.collectFirst {
      case id: UserRef => id
    } orElse (caller.identities.collectFirst {
      case id: AuthenticatedRef => id
    }) getOrElse (Identity.Anonymous)
}

object CallerCtx {

  /**
    * Summons a [[CallerCtx]] instance from the implicit scope.
    *
    * @param instance the implicitly available instance
    */
  implicit final def apply(implicit instance: CallerCtx): CallerCtx = instance

  /**
    * Creates a [[CallerCtx]] from the implicitly available ''clock'' and ''identity''.
    *
    * @param clock  the implicitly available clock to issue instants
    * @param caller the implicitly available author
    */
  implicit def fromImplicit(implicit clock: Clock, caller: Caller): CallerCtx = CallerCtx(clock, caller)
}
