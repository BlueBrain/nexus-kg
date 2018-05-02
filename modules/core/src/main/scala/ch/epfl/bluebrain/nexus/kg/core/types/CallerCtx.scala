package ch.epfl.bluebrain.nexus.kg.core.types

import java.time.Clock

import ch.epfl.bluebrain.nexus.commons.types.Meta
import ch.epfl.bluebrain.nexus.commons.types.identity.Identity
import ch.epfl.bluebrain.nexus.commons.types.identity.Identity.{AuthenticatedRef, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.Caller
/**
  * Context information for any operation bundle call
  *
  * @param caller the author of the operation call
  */
final case class CallerCtx(caller: Caller) {
  private lazy val ident          = identity()
  def meta(implicit clock: Clock) = Meta(ident, clock.instant())

  private def identity(): Identity =
    caller.identities.collectFirst {
      case id: UserRef => id
    } orElse caller.identities.collectFirst {
      case id: AuthenticatedRef => id
    } getOrElse Identity.Anonymous()
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
    * @param caller the implicitly available author
    */
  implicit def fromImplicit(implicit caller: Caller): CallerCtx = CallerCtx(caller)
}
