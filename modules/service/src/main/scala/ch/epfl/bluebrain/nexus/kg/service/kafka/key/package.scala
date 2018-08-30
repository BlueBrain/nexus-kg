package ch.epfl.bluebrain.nexus.kg.service.kafka

package object key {

  /**
    * Implicit class to provide a ''key'' extension method given the presence
    * of a [[Key]] instance.
    */
  implicit class KeyOps[A](a: A)(implicit tc: Key[A]) {
    def key: String = tc.key(a)
  }
}
