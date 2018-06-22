package ch.epfl.bluebrain.nexus.kg.async

import akka.cluster.ddata.LWWRegister.Clock

/**
  * A value with an attached revision corresponding to the value.
  *
  * @param rev   the value revision
  * @param value the value
  */
final case class RevisionedValue[A](rev: Long, value: A)

object RevisionedValue {

  private[async] val clock: Clock[RevisionedValue[_]] =
    (_: Long, value: RevisionedValue[_]) => value.rev
}
