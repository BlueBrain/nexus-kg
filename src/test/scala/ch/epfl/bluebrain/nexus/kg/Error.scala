package ch.epfl.bluebrain.nexus.kg

import shapeless.Typeable

/**
  * An error representation that can be uniquely identified by its code.
  *
  * @param code       the unique code for this error
  * @param message    an optional detailed error message
  * @param `@context` the JSON-LD context
  */
final case class Error(code: String, message: Option[String], `@context`: String)

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
