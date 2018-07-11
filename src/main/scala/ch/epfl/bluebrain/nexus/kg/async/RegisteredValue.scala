package ch.epfl.bluebrain.nexus.kg.async

/**
  * A value with being registered into a [[akka.cluster.ddata.LWWRegister]].
  */
trait RegisteredValue[A] {
  def value: A
}
