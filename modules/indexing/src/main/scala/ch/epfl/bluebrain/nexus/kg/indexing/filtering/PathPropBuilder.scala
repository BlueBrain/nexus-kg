package ch.epfl.bluebrain.nexus.kg.indexing.filtering

import akka.http.scaladsl.model.Uri
import cats.MonadError
import ch.epfl.bluebrain.nexus.common.types.Err
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PathProp.{AlternativeSeqPath, FollowSeqPath, InversePath, PathOneOrMore, PathZeroOrMore, PathZeroOrOne, UriPath}
import org.apache.jena.sparql.path._
import cats.syntax.flatMap._
import cats.syntax.functor._
import scala.util.Try

trait PathPropBuilder[F[_], A]{
  def apply(path: A)(implicit F: MonadError[F, Throwable]): F[PathProp]
}

object PathPropBuilder {

  final case class PathPropError(path: Path*) extends Err("Error building the paths")

  final def apply[F[_], A](implicit instance: PathPropBuilder[F, A]): PathPropBuilder[F, A] = instance

  implicit def jenaPathPropBuilder[F[_]](implicit F: MonadError[F, Throwable]) = new PathPropBuilder[F, Path] {

    private def resolvePaths(left: Path, right: Path): F[(PathProp, PathProp)] =
      for {
        leftPath <- apply(left)
        rightPath <- apply(right)
      } yield (leftPath, rightPath)

    override def apply(path: Path)(implicit F: MonadError[F, Throwable]): F[PathProp] =
      path match {
        case LinkExtr(uri)                 => F.pure(UriPath(uri))
        case ZeroOrOneExtr(LinkExtr(uri))  => F.pure(PathZeroOrOne(uri))
        case ZeroOrMoreExtr(LinkExtr(uri)) => F.pure(PathZeroOrMore(uri))
        case OneOrMoreExtr(LinkExtr(uri))  => F.pure(PathOneOrMore(uri))
        case InverseExtr(LinkExtr(uri))    => F.pure(InversePath(uri))
        case SeqExtr(left, right)          => resolvePaths(left, right) map { case (l, r) => FollowSeqPath(l, r) }
        case AltExtr(left, right)          => resolvePaths(left, right) map { case (l, r) => AlternativeSeqPath(l, r) }
        case _                             => F.raiseError(PathPropError(path))

      }
  }
}

object LinkExtr {
  def unapply(value: Path): Option[Uri] = value match {
    case link: P_Link =>
      Try(Uri(link.getNode.getURI)).toOption
        .filter(uri => uri.isAbsolute && uri.toString().indexOf("/") > -1)
    case _            =>
      None
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
    case _           =>
      None
  }
}

object AltExtr {
  def unapply(value: Path): Option[(Path, Path)] = value match {
    case link: P_Alt => Some(link.getLeft -> link.getRight)
    case _           =>
      None
  }
}