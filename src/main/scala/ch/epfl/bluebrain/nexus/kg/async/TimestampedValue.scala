package ch.epfl.bluebrain.nexus.kg.async

import akka.cluster.ddata.LWWRegister.Clock

/**
  * A value with an attached timestamp when it was produced.
  *
  * @param timestamp the instant when the value was produced
  * @param value     the value
  */
final case class TimestampedValue[A](timestamp: Long, value: A)

object TimestampedValue {

  private[async] def timestampedValueClock[A]: Clock[TimestampedValue[A]] =
    (_: Long, value: TimestampedValue[A]) => value.timestamp
}
