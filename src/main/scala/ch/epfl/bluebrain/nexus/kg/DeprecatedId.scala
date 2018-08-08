package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * A type class that provides a specific deprecated identifier (''id: AbsoluteIri'' and ''deprecated: Boolean'') of a resource of type ''A''.
  */
sealed trait DeprecatedId[A] {

  /**
    * @return the unique identifier of the resource 'a'
    */
  def id(a: A): AbsoluteIri

  /**
    * @return the deprecation flag of the identified resource 'a'
    */
  def deprecated(a: A): Boolean
}

object DeprecatedId {

  /**
    * Constructor of [[DeprecatedId]] from a tuple
    *
    * @param f a function that returns a [[Tuple2]] of [[AbsoluteIri]] and [[Boolean]]
    */
  final def apply[A](f: A => (AbsoluteIri, Boolean)): DeprecatedId[A] = new DeprecatedId[A] {
    override def id(a: A)         = f(a)._1
    override def deprecated(a: A) = f(a)._2
  }
  implicit class DeprecatedIdSyntax[A](private val value: A)(implicit R: DeprecatedId[A]) {
    def id: AbsoluteIri     = R.id(value)
    def deprecated: Boolean = R.deprecated(value)
  }
}
