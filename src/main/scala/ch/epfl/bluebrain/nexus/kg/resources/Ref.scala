package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * An annotated reference.
  *
  * @param iri the reference identifier as an iri
  * @param ann the annotation value
  * @tparam A the type of the annotation
  */
final case class Ref[A](iri: AbsoluteIri, ann: A) {

  /**
    * Transforms the annotation with the argument ''f''.
    *
    * @param f  the transformation function
    * @tparam B the resulting type of the annotation
    * @return a new ''Ref'' value with the same ''iri'' and an annotation computed by applying the ''f'' on ''this''
    *         annotation
    */
  def map[B](f: A => B): Ref[B] =
    Ref(iri, f(ann))

  /**
    * @return an unannotated reference keeping ''this'' iri value.
    */
  def drop: Ref[Unit] =
    map(_ => ())
}

object Ref {

  /**
    * A reference annotated with a revision.
    */
  type Revision = Ref[Long]

  object Revision {
    final def unapply(value: Ref[_]): Option[Revision] = value match {
      case r @ Ref(_, rev: Long) if rev >= 0 => Some(r.asInstanceOf[Revision])
      case _                                 => None
    }
  }

  /**
    * A reference annotated with a string tag.
    */
  type Tag = Ref[String]

  object Tag {
    final def unapply(value: Ref[_]): Option[Tag] = value match {
      case r @ Ref(_, _: String) => Some(r.asInstanceOf[Tag])
      case _                     => None
    }
  }

  /**
    * An unannotated reference.
    */
  type Latest = Ref[Unit]

  object Latest {
    final def unapply(value: Ref[_]): Option[Latest] = value match {
      case r @ Ref(_, _: Unit) => Some(r.asInstanceOf[Latest])
      case _                   => None
    }
  }

  /**
    * Constructs a reference annotated with a revision.
    *
    * @param iri the reference identifier as an iri
    * @param rev the reference revision
    */
  final def revision(iri: AbsoluteIri, rev: Long): Revision =
    Ref(iri, rev)

  /**
    * Constructs a reference annotated with a tag.
    *
    * @param iri the reference identifier as an iri
    * @param tag the reference tag
    */
  final def tag(iri: AbsoluteIri, tag: String): Tag =
    Ref(iri, tag)

  /**
    * Constructs an unannotated reference.
    *
    * @param iri the reference identifier as an iri
    */
  final def latest(iri: AbsoluteIri): Latest =
    Ref(iri, ())
}
