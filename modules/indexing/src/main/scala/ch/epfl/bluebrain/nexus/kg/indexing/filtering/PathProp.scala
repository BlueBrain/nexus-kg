package ch.epfl.bluebrain.nexus.kg.indexing.filtering

import akka.http.scaladsl.model.Uri
import cats.Show
import cats.syntax.show._

sealed trait PathProp extends Product with Serializable

object PathProp {

  /**
    * Path property which represents a URI or a prefixed name
    *
    * @param value the URI or a prefixed name.
    */
  final case class UriPath(value: Uri) extends PathProp

  /**
    * Inverse path (object to subject). Sparql expression: ^value
    *
    * @param value the URI or a prefixed name.
    */
  final case class InversePath(value: Uri) extends PathProp

  /**
    * A group path ''value''. Sparql expression: (value)
    *
    * @param value the URI or a prefixed name.
    */
  final case class GroupPath(value: Uri) extends PathProp

  /**
    * A path of zero or more occurrences of ''value''. Sparql expression: value*
    *
    * @param value the URI or a prefixed name.
    */
  final case class PathZeroOrMore(value: Uri) extends PathProp

  /**
    * A path of one or more occurrences of ''value''. Sparql expression: value+
    *
    * @param value the URI or a prefixed name.
    */
  final case class PathOneOrMore(value: Uri) extends PathProp

  /**
    * A path of zero or one occurrences of ''value''. Sparql expression: value?
    *
    * @param value the URI or a prefixed name.
    */
  final case class PathZeroOrOne(value: Uri) extends PathProp

  /**
    * A sequence path of ''left'', followed by ''right''. Sparql expression: left / right
    *
    * @param left  the left hand side ''path''
    * @param right the right hand side ''path''
    */
  final case class FollowSeqPath(left: PathProp, right: PathProp) extends PathProp

  /**
    * A alternative path of ''left''or ''right''. Sparql expression: left | right
    *
    * @param left  the left hand side ''path''
    * @param right the right hand side ''path''
    */
  final case class AlternativeSeqPath(left: PathProp, right: PathProp) extends PathProp

  private implicit val showUriPath: Show[Uri] = Show.show(uri => s"<$uri>")

  implicit val showPath: Show[PathProp] = Show.show[PathProp] {
    case UriPath(uri)                         => uri.show
    case InversePath(uri)                     => s"^${uri.show}"
    case GroupPath(uri)                       => s"(${uri.show})"
    case PathZeroOrMore(uri)                  => s"(${uri.show})*"
    case PathOneOrMore(uri)                   => s"(${uri.show})+"
    case PathZeroOrOne(uri)                   => s"(${uri.show})?"
    case FollowSeqPath(first, second)         => s"${showPath.show(first)}/${showPath.show(second)}"
    case AlternativeSeqPath(first, second)    => s"${showPath.show(first)}|${showPath.show(second)}"

  }
}
