package ch.epfl.bluebrain.nexus.kg.service.kafka.key

/**
  * Type class loosely based on [[cats.Show]] for a generic type ''A''
  * to explicitly provide a ''key''.
  */
trait Key[A] {
  def key(a: A): String
}

object Key {

  /**
    * Creates an instance of [[Key]] using the provided function ''f''.
    */
  def key[A](f: A => String): Key[A] = (a: A) => f(a)

  /**
    * Creates an instance of [[Key]] using the object toString function.
    */
  def fromToString[A]: Key[A] = (a: A) => a.toString

}
