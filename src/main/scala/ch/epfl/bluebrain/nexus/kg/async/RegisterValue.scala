package ch.epfl.bluebrain.nexus.kg.async

import akka.cluster.ddata.LWWRegister.Clock

/**
  * A value with being registered into a [[akka.cluster.ddata.LWWRegister]].
  */
sealed trait RegisterValue[A] {
  def value: A
}
object RegisterValue {

  /**
    * A value with an attached revision corresponding to the value.
    *
    * @param rev   the value revision
    * @param value the value
    */
  final case class RevisionedValue[A](rev: Long, value: A) extends RegisterValue[A]

  object RevisionedValue {

    private[async] def revisionedValueClock[A]: Clock[RevisionedValue[A]] =
      (_: Long, value: RevisionedValue[A]) => value.rev
  }

  /**
    * A value with an attached timestamp when it was produced.
    *
    * @param timestamp the instant when the value was produced
    * @param value     the value
    */
  final case class TimestampedValue[A](timestamp: Long, value: A) extends RegisterValue[A]

  object TimestampedValue {

    private[async] def timestampedValueClock[A]: Clock[TimestampedValue[A]] =
      (_: Long, value: TimestampedValue[A]) => value.timestamp
  }
}
