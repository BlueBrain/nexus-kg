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

  private[async] val clock: Clock[TimestampedValue[_]] =
    (_: Long, value: TimestampedValue[_]) => value.timestamp
}
