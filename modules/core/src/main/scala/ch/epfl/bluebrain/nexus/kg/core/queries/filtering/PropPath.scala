package ch.epfl.bluebrain.nexus.kg.core.queries.filtering

import akka.http.scaladsl.model.Uri
import cats.syntax.show._
import cats.{Eval, Show}
import ch.epfl.bluebrain.nexus.commons.types.Err
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath._
import org.apache.jena.graph.Node
import org.apache.jena.sparql.path._

import scala.collection.JavaConverters._
import scala.util.Try

sealed trait PropPath extends Product with Serializable

object PropPath extends PropPathBuilder {

  /**
    * Path property which represents a URI.
    *
    * @param value the URI
    */
  final case class UriPath(value: Uri) extends PropPath

  /**
    * Inverse path (object to subject). Sparql expression: ^value
    *
    * @param value the URI
    */
  final case class InversePath(value: Uri) extends PropPath

  /**
    * A group path ''value''. Sparql expression: (value)
    *
    * @param value the URI
    */
  final case class GroupPath(value: Uri) extends PropPath

  /**
    * A path of zero or more occurrences of ''value''. Sparql expression: value*
    *
    * @param value the URI
    */
  final case class PathZeroOrMore(value: Uri) extends PropPath

  /**
    * A path of one or more occurrences of ''value''. Sparql expression: value+
    *
    * @param value the URI
    */
  final case class PathOneOrMore(value: Uri) extends PropPath

  /**
    * A path of zero or one occurrences of ''value''. Sparql expression: value?
    *
    * @param value the URI
    */
  final case class PathZeroOrOne(value: Uri) extends PropPath

  /**
    * A sequence path of ''left'', followed by ''right''. Sparql expression: left / right
    *
    * @param left  the left hand side ''path''
    * @param right the right hand side ''path''
    */
  final case class SeqPath(left: PropPath, right: PropPath) extends PropPath

  /**
    * A alternative path of ''left''or ''right''. Sparql expression: left | right
    *
    * @param left  the left hand side ''path''
    * @param right the right hand side ''path''
    */
  final case class AlternativeSeqPath(left: PropPath, right: PropPath) extends PropPath

  /**
    * A negated seq. Sparql expression: !iri or !(iri1| ...|irin)
    *
    * @param paths list of [[PropPath]]
    */
  final case class NegatedSeqPath(paths: List[PropPath]) extends PropPath

  private implicit val showUriPath: Show[Uri] = Show.show(uri => s"<$uri>")

  implicit val showPath: Show[PropPath] = Show.show[PropPath] {
    case UriPath(uri)                      => uri.show
    case InversePath(uri)                  => s"^${uri.show}"
    case GroupPath(uri)                    => s"(${uri.show})"
    case PathZeroOrMore(uri)               => s"(${uri.show})*"
    case PathOneOrMore(uri)                => s"(${uri.show})+"
    case PathZeroOrOne(uri)                => s"(${uri.show})?"
    case SeqPath(first, second)            => s"${showPath.show(first)}/${showPath.show(second)}"
    case AlternativeSeqPath(first, second) => s"${showPath.show(first)}|${showPath.show(second)}"
    case NegatedSeqPath(Nil)               => ""
    case NegatedSeqPath(list) =>
      list
        .foldLeft(Vector.empty[String]) { (acc, current) =>
          acc :+ showPath.show(current)
        }
        .mkString("!(", "|", ")")

  }

  /**
    * Signals that the ''PathProp'' cannot be created for the given ''path''s
    * @param path the [[Path]]s for which the ''PathProp'' was attempted to be created
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class PropPathError(path: Path*) extends Err("Error building the paths")
}

trait PropPathBuilder {

  /**
    * Attempts to convert a Jena ''path'' into a PathProp. If it fails, it returns a [[PropPathError]]
    *
    * @param path the parsed path to be converted
    */
  final def fromJena(path: Path): Either[PropPathError, PropPath] = {
    def two(left: Path, right: Path, f: (PropPath, PropPath) => PropPath): Eval[Either[PropPathError, PropPath]] = {
      val value = for {
        l <- inner(left)
        r <- inner(right)
      } yield (l, r)
      value map {
        case (Right(ll), Right(rr)) => Right(f(ll, rr))
        case _                      => Left(PropPathError(left, right))
      }
    }

    def many(paths: List[Path]): Either[PropPathError, PropPath] =
      paths.foldLeft[Either[PropPathError, List[PropPath]]](Right(Nil)) {
        case (l @ Left(_), _)  => l
        case (Right(list), el) => inner(el).value.map(_ :: list)
      } match {
        case Left(_)     => Left(PropPathError(paths: _*))
        case Right(list) => Right(NegatedSeqPath(list.reverse))
      }

    def inner(path: Path): Eval[Either[PropPathError, PropPath]] = {
      path match {
        case LinkExtr(uri)                 => Eval.now(Right(UriPath(uri)))
        case ZeroOrOneExtr(LinkExtr(uri))  => Eval.now(Right(PathZeroOrOne(uri)))
        case ZeroOrMoreExtr(LinkExtr(uri)) => Eval.now(Right(PathZeroOrMore(uri)))
        case OneOrMoreExtr(LinkExtr(uri))  => Eval.now(Right(PathOneOrMore(uri)))
        case InverseExtr(LinkExtr(uri))    => Eval.now(Right(InversePath(uri)))
        case ReverseExtr(uri)              => Eval.now(Right(InversePath(uri)))
        case SeqExtr(left, right)          => two(left, right, SeqPath.apply)
        case AltExtr(left, right)          => two(left, right, AlternativeSeqPath.apply)
        case NegSeqExtr(paths)             => Eval.later(many(paths))

        case _ => Eval.now(Left(PropPathError(path)))
      }
    }

    inner(path).value
  }

  private implicit def toUri(node: Node): Option[Uri] =
    Try(Uri(node.getURI)).toOption
      .filter(uri => uri.isAbsolute && uri.toString().indexOf("/") > -1)

  object LinkExtr {
    def unapply(value: Path): Option[Uri] = value match {
      case link: P_Link => link.getNode
      case _            => None
    }
  }

  object ZeroOrOneExtr {
    def unapply(value: Path): Option[Path] = value match {
      case link: P_ZeroOrOne => Some(link.getSubPath)
      case _                 => None
    }
  }

  object InverseExtr {
    def unapply(value: Path): Option[Path] = value match {
      case link: P_Inverse => Some(link.getSubPath)
      case _               => None
    }
  }

  object ReverseExtr {
    def unapply(value: Path): Option[Uri] = value match {
      case link: P_ReverseLink => link.getNode
      case _                   => None
    }
  }

  object OneOrMoreExtr {
    def unapply(value: Path): Option[Path] = value match {
      case link: P_OneOrMore1 => Some(link.getSubPath)
      case _                  => None
    }
  }

  object ZeroOrMoreExtr {
    def unapply(value: Path): Option[Path] = value match {
      case link: P_ZeroOrMore1 => Some(link.getSubPath)
      case _                   => None
    }
  }

  object SeqExtr {
    def unapply(value: Path): Option[(Path, Path)] = value match {
      case link: P_Seq => Some(link.getLeft -> link.getRight)
      case _           => None
    }
  }

  object AltExtr {
    def unapply(value: Path): Option[(Path, Path)] = value match {
      case link: P_Alt => Some(link.getLeft -> link.getRight)
      case _           => None
    }
  }

  object NegSeqExtr {
    def unapply(value: Path): Option[List[Path]] = value match {
      case link: P_NegPropSet => Some(link.getNodes.asScala.toList)
      case _                  => None
    }
  }
}
