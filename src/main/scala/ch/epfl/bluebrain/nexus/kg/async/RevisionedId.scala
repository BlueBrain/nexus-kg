package ch.epfl.bluebrain.nexus.kg.async

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * A type class of ''A'' which has the methods ''id'' and ''rev''
  * with the provided signature
  */
sealed trait RevisionedId[A] {
  def id(a: A): AbsoluteIri
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
