package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.data.{EitherT, OptionT}
import cats.implicits._
import cats.{Applicative, Show}
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller, Permission}
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.{ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{ProjectLabelNotFound, ProjectRefNotFound}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import io.circe.syntax._
import io.circe.{Decoder, Encoder}

import scala.util.{Success, Try}

/**
  * Enumeration of project identifier types
  */
sealed trait ProjectIdentifier extends Product with Serializable {
  /**
    * Attempts to convert the current ProjectIdentifier to a [[ProjectLabel]] using the passed cache
    *
    * @tparam F the effect type
    */
  def toLabel[F[_]: Applicative](implicit cache: ProjectCache[F]): EitherT[F, Rejection, ProjectLabel]

  /**
    * Attempts to convert the current ProjectIdentifier to a [[ProjectRef]] using the passed cache
    *
    * @tparam F the effect type
    */
  def toRef[F[_]: Applicative](implicit cache: ProjectCache[F]): EitherT[F, Rejection, ProjectRef]

  /**
    * Attempts to convert the current ProjectIdentifier to a [[ProjectRef]] using the passed cache.
    * Before the conversion is applied, the passed ''caller'' must have the passed ''perm'' inside the ''acls'' path.
    *
    * @tparam F the effect type
    */
  def toRef[F[_]: Applicative](
      perm: Permission
  )(implicit acls: AccessControlLists, caller: Caller, cache: ProjectCache[F]): EitherT[F, Rejection, ProjectRef]

}

object ProjectIdentifier {

  /**
    * Representation of the project label, containing both the organization and the project segments
    *
    * @param organization the organization segment of the label
    * @param value        the project segment of the label
    */
  final case class ProjectLabel(organization: String, value: String) extends ProjectIdentifier {
    lazy val notFound: Rejection = ProjectRefNotFound(this)
    def toLabel[F[_]: Applicative](implicit cache: ProjectCache[F]): EitherT[F, Rejection, ProjectLabel] =
      EitherT.rightT(this)

    def toRef[F[_]: Applicative](implicit cache: ProjectCache[F]): EitherT[F, Rejection, ProjectRef] =
      OptionT(cache.get(this)).map(_.ref).toRight(notFound)

    def toRef[F[_]: Applicative](
        perm: Permission
    )(implicit acls: AccessControlLists, caller: Caller, cache: ProjectCache[F]): EitherT[F, Rejection, ProjectRef] =
      if (caller.hasPermission(acls, this, perm)) toRef
      else EitherT.leftT(notFound)

  }

  object ProjectLabel {
    implicit val segmentShow: Show[ProjectLabel] = Show.show(s => s"${s.organization}/${s.value}")
    implicit val projectLabelEncoder: Encoder[ProjectLabel] =
      Encoder.encodeString.contramap(_.show)

    implicit val projectLabelDecoder: Decoder[ProjectLabel] = Decoder.decodeString.emap { s =>
      s.split("/", 2) match {
        case Array(org, project) if !project.contains("/") => Right(ProjectLabel(org, project))
        case _                                             => Left(s"'$s' cannot be converted to ProjectLabel")
      }
    }

    implicit val projectLabelNodeEncoder: NodeEncoder[ProjectLabel] = node =>
      NodeEncoder.stringEncoder(node).flatMap { value =>
        value.trim.split("/") match {
          case Array(organization, project) => Right(ProjectLabel(organization, project))
          case _                            => Left(IllegalConversion("Expected a ProjectLabel, but found otherwise"))
        }
      }
  }

  /**
    * A stable project reference.
    *
    * @param id the underlying stable identifier for a project
    */
  final case class ProjectRef(id: UUID) extends ProjectIdentifier {
    private lazy val notFound: Rejection = ProjectLabelNotFound(this)

    def toLabel[F[_]: Applicative](implicit cache: ProjectCache[F]): EitherT[F, Rejection, ProjectLabel] =
      OptionT(cache.getLabel(this)).toRight(notFound)

    def toRef[F[_]: Applicative](implicit cache: ProjectCache[F]): EitherT[F, Rejection, ProjectRef] =
      EitherT.rightT(this)

    def toRef[F[_]: Applicative](
        perm: Permission
    )(implicit acls: AccessControlLists, caller: Caller, cache: ProjectCache[F]): EitherT[F, Rejection, ProjectRef] =
      toRef

  }

  object ProjectRef {

    implicit val projectRefShow: Show[ProjectRef] = Show.show(_.id.toString)
    implicit val projectRefEncoder: Encoder[ProjectRef] =
      Encoder.encodeString.contramap(_.show)

    implicit val projectRefDecoder: Decoder[ProjectRef] =
      Decoder.decodeString.emapTry(uuid => Try(UUID.fromString(uuid)).map(ProjectRef.apply))

    implicit val projectRefNodeEncoder: NodeEncoder[ProjectRef] = node =>
      NodeEncoder.stringEncoder(node).flatMap { value =>
        Try(UUID.fromString(value)) match {
          case Success(uuid) => Right(ProjectRef(uuid))
          case _             => Left(IllegalConversion("Expected a ProjectRef, but found otherwise"))
        }
      }
  }

  implicit val projectIdentifierEncoder: Encoder[ProjectIdentifier] =
    Encoder.encodeJson.contramap {
      case value: ProjectLabel => value.asJson
      case value: ProjectRef   => value.asJson
    }

  implicit val projectIdentifierDecoder: Decoder[ProjectIdentifier] =
    Decoder.instance { h =>
      ProjectRef.projectRefDecoder(h) match {
        case Left(_) => ProjectLabel.projectLabelDecoder(h)
        case other   => other
      }
    }

  implicit val projectIdentifierNodeEncoder: NodeEncoder[ProjectIdentifier] = node =>
    ProjectRef.projectRefNodeEncoder(node) match {
      case Left(_) => ProjectLabel.projectLabelNodeEncoder(node)
      case other   => other
    }

  implicit val projectIdentifierShow: Show[ProjectIdentifier] =
    Show.show {
      case value: ProjectLabel => value.show
      case value: ProjectRef   => value.show
    }
}
