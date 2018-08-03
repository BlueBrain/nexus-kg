package ch.epfl.bluebrain.nexus.kg.async

/**
  * A value with an attached revision corresponding to the value.
  *
  * @param rev   the value revision
  * @param value the value
  */
final case class RevisionedValue[A](rev: Long, value: A) extends RegisteredValue[A]
