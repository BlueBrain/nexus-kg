package ch.epfl.bluebrain.nexus.kg.async

import akka.cluster.ddata.LWWRegister.Clock

/**
  * A value with an attached revision corresponding to the value.
  *
  * @param rev   the value revision
  * @param value the value
  */
final case class RevisionValue[A](rev: Long, value: A)

object RevisionValue {

  private[async] def revisionedValueClock[A]: Clock[RevisionValue[A]] =
    (_: Long, value: RevisionValue[A]) => value.rev
}
