package ch.epfl.bluebrain.nexus.kg.resources

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Query, Url, Urn}

import scala.util.Try

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
    * Constructs a reference from the argument iri. If the iri contains ''_rev'' or ''_tag'' query parameters their
    * values are used to refine the reference and stripped from the original iri.
    *
    * @param iri the iri to lift into a reference
    */
  final def apply(iri: AbsoluteIri): Ref = {
    def extractTagRev(q: Query): (Query, Option[Either[String, Long]]) = {
      val map = q.value
      def rev = map.get("_rev").flatMap(_.headOption).flatMap(s => Try(s.toLong).filter(_ > 0).toOption)
      def tag = map.get("_tag").flatMap(_.headOption).filter(_.nonEmpty)
      (Query(map - "_tag" - "_rev"), rev.map(Right.apply) orElse tag.map(Left.apply))
    }
    def refOf(id: AbsoluteIri, opt: Option[Either[String, Long]]): Ref = opt match {
      case Some(Left(tag))  => Tag(id, tag)
      case Some(Right(rev)) => Revision(id, rev)
      case _                => Latest(id)
    }

    iri match {
      case u @ Url(_, _, _, Some(query), _) =>
        val (q, opt) = extractTagRev(query)
        refOf(u.copy(query = Some(q)), opt)
      case u @ Urn(_, _, _, Some(query), _) =>
        val (q, opt) = extractTagRev(query)
        refOf(u.copy(q = Some(q)), opt)
      case _ =>
        Latest(iri)
    }
  }

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
