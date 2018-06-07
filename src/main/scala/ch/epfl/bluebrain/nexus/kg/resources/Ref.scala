package ch.epfl.bluebrain.nexus.kg.resources

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

/**
  * A resource reference.
  */
sealed trait Ref extends Product with Serializable {

  /**
    * @return the reference identifier as an iri
    */
  def iri: AbsoluteIri
}

object Ref {

  /**
    * An unannotated reference.
    * @param iri the reference identifier as an iri
    */
  final case class Latest(iri: AbsoluteIri) extends Ref

  /**
    * A reference annotated with a revision.
    *
    * @param iri the reference identifier as an iri
    * @param rev the reference revision
    */
  final case class Revision(iri: AbsoluteIri, rev: Long) extends Ref

  /**
    * A reference annotated with a tag.
    *
    * @param iri the reference identifier as an iri
    * @param tag the reference tag
    */
  final case class Tag(iri: AbsoluteIri, tag: String) extends Ref

  final implicit val refShow: Show[Ref] = Show.show {
    case Latest(iri)        => iri.show
    case Tag(iri, tag)      => s"${iri.show} @ tag: '$tag'"
    case Revision(iri, rev) => s"${iri.show} @ rev: '$rev'"
  }
}
