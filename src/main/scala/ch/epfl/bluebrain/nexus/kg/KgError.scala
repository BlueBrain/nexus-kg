package ch.epfl.bluebrain.nexus.kg

import akka.http.scaladsl.model.StatusCodes
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.config.Contexts.errorCtxUri
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectLabel, Ref}
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.service.http.directives.StatusFrom
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveEncoder
import io.circe.{Encoder, Json}

/**
  * Enumeration of runtime errors.
  *
  * @param msg a description of the error
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
sealed abstract class KgError(val msg: String) extends Exception with Product with Serializable {
  override def fillInStackTrace(): KgError = this
  override def getMessage: String          = msg
}

@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
object KgError {

  /**
    * Generic wrapper for kg errors that should not be exposed to clients.
    *
    * @param reason the underlying error reason
    */
  final case class InternalError(reason: String) extends KgError(reason)

  /**
    * Signals that the requested resource was not found
    */
  final case class NotFound(ref: Option[String] = None) extends KgError("The requested resource could not be found.")
  object NotFound {
    def apply(ref: Ref): NotFound =
      NotFound(Some(ref.show))
  }

  /**
    * Signals that the provided authentication is not valid.
    */
  final case object AuthenticationFailed extends KgError("The supplied authentication is invalid.")

  /**
    * Signals that the provided output format name is not valid.
    *
    * @param name the provided output format name
    */
  final case class InvalidOutputFormat(name: String) extends KgError(s"The supplied output format '$name' is invalid.")

  /**
    * Signals that the caller doesn't have access to the selected resource.
    */
  final case object AuthorizationFailed
      extends KgError("The supplied authentication is not authorized to access this resource.")

  /**
    * Signals the inability to connect to an underlying service to perform a request.
    *
    * @param msg a human readable description of the cause
    */
  final case class DownstreamServiceError(override val msg: String) extends KgError(msg)

  /**
    * Signals an internal timeout.
    *
    * @param msg a descriptive message on the operation that timed out
    */
  final case class OperationTimedOut(override val msg: String) extends KgError(msg)

  /**
    * Signals the impossibility to resolve the project reference for project labels.
    *
    * @param label the project label
    */
  final case class ProjectNotFound(label: ProjectLabel) extends KgError(s"Project '${label.show}' not found.")

  /**
    * Signals an attempt to interact with a resource that belongs to a deprecated project.
    *
    * @param ref a reference to the project
    */
  final case class ProjectIsDeprecated(ref: ProjectLabel) extends KgError(s"Project '${ref.show}' is deprecated.")

  implicit val kgErrorEncoder: Encoder[KgError] = {
    implicit val config: Configuration = Configuration.default.withDiscriminator("@type")
    val enc                            = deriveEncoder[KgError].mapJson(_ addContext errorCtxUri)
    Encoder.instance(r => enc(r) deepMerge Json.obj("reason" -> Json.fromString(r.msg)))
  }

  implicit val kgErrorStatusFrom: StatusFrom[KgError] = {
    case _: NotFound            => StatusCodes.NotFound
    case AuthenticationFailed   => StatusCodes.Unauthorized
    case AuthorizationFailed    => StatusCodes.Forbidden
    case _: ProjectIsDeprecated => StatusCodes.BadRequest
    case _: InvalidOutputFormat => StatusCodes.BadRequest
    case _: ProjectNotFound     => StatusCodes.NotFound
    case _                      => StatusCodes.InternalServerError
  }
}
