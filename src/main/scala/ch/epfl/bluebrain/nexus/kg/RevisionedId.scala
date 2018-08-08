package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * A type class that provides a specific revision identifier (''id: AbsoluteIri'' and ''rev: Long'') of a resource of type ''A''.
  */
sealed trait RevisionedId[A] {

  /**
    * @return the unique identifier of the resource 'a'
    */
  def id(a: A): AbsoluteIri

  /**
    * @return the revision number of the identified resource 'a'
    */
  def rev(a: A): Long
}
object RevisionedId {

  /**
    * Constructor of [[RevisionedId]] from a tuple
    *
    * @param f a function that returns a [[Tuple2]] of [[AbsoluteIri]] and [[Long]]
    */
  final def apply[A](f: A => (AbsoluteIri, Long)): RevisionedId[A] = new RevisionedId[A] {
    override def id(a: A)  = f(a)._1
    override def rev(a: A) = f(a)._2
  }
  implicit class RevisionedIdSyntax[A](private val value: A)(implicit R: RevisionedId[A]) {
    def id: AbsoluteIri = R.id(value)
    def rev: Long       = R.rev(value)
  }
}
