package ch.epfl.bluebrain.nexus.kg

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.ContextUri
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.kg.config.Contexts.{errorCtxUri, _}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import io.circe.Encoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveEncoder
import io.circe.refined._

package object marshallers {

  private implicit def aEncoder[A: Show]: Encoder[A] = Encoder.encodeString.contramap(_.show)

  private[marshallers] implicit val errorContext: ContextUri = errorCtxUri

  private[marshallers] val rejectionEncoder: Encoder[Rejection] = {
    import io.circe.generic.auto._
    val enc = deriveEncoder[Rejection]
    Encoder(enc(_).addContext(errorContext))
  }

  private[marshallers] def statusCodeFrom(rejection: Rejection): StatusCode = rejection match {
    case _: IsDeprecated | _: ProjectIsDeprecated | _: UpdateSchemaTypes | _: IncorrectTypes | _: IllegalContextValue |
        _: UnableToSelectResourceId | _: InvalidResource | _: IncorrectId | _: InvalidPayload | _: IllegalParameter |
        _: MissingParameter =>
      StatusCodes.BadRequest
    case _: UnexpectedState | _: Unexpected                       => StatusCodes.InternalServerError
    case _: NotFound | _: AttachmentNotFound | _: ProjectNotFound => StatusCodes.NotFound
    case _: IncorrectRev | _: AlreadyExists                       => StatusCodes.Conflict
    case _: DownstreamServiceError                                => StatusCodes.BadGateway
  }

  /**
    * The discriminator is enough to give us a Json representation (the name of the class)
    */
  private[marshallers] implicit val rejectionConfig: Configuration = Configuration.default.withDiscriminator("code")

}
