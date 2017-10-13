package ch.epfl.bluebrain.nexus.kg.service.routes

import shapeless.Typeable

/**
  * An error representation that can be uniquely identified by its code.
  *
  * @param code the unique code for this error
  */
final case class Error(code: String)

object Error {

  /**
    * Provides the class name for ''A''s that have a [[shapeless.Typeable]] typeclass instance.
    *
    * @tparam A a generic type parameter
    * @return class name of A
    */
  final def classNameOf[A: Typeable]: String = {
    val describe = implicitly[Typeable[A]].describe
    describe.substring(0, describe.lastIndexOf('.'))
  }
}
