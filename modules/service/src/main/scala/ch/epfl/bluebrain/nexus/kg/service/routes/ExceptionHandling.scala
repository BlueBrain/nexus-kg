package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{EntityStreamSizeException, Uri}
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import ch.epfl.bluebrain.nexus.commons.service.directives.ErrorDirectives._
import ch.epfl.bluebrain.nexus.commons.service.directives.StatusFrom
import ch.epfl.bluebrain.nexus.kg.core.Fault.{CommandRejected, Unexpected}
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRejection
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRejection
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRejection.AttachmentLimitExceeded
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection
import ch.epfl.bluebrain.nexus.kg.service.io.RoutesEncoder.JsonLDKeys.`@context`
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalFilterFormat
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.{Encoder, Json}
import journal.Logger

/**
  * Total exception handling logic for the service.
  * It provides an exception handler implementation that ensures
  * all rejections and unexpected failures are gracefully handled
  * and presented to the caller.
  */
class ExceptionHandling(errorContext: Uri) {

  val logger = Logger[this.type]

  private final def exceptionHandler: ExceptionHandler = ExceptionHandler {
    case CommandRejected(r: InstanceRejection)   => complete(r)
    case CommandRejected(r: SchemaRejection)     => complete(r)
    case CommandRejected(r: ContextRejection)    => complete(r)
    case CommandRejected(r: DomainRejection)     => complete(r)
    case CommandRejected(r: OrgRejection)        => complete(r)
    case CommandRejected(r: IllegalFilterFormat) => complete(r)
    case ex: EntityStreamSizeException =>
      logger.warn(s"An attachment with size '${ex.actualSize}' has been rejected because actual limit is '${ex.limit}'")
      complete(toRejection(ex))
    // $COVERAGE-OFF$
    case Unexpected(reason) =>
      logger.warn(s"An unexpected rejection has happened '$reason'")
      complete(InternalError())
    // $COVERAGE-ON$
  }

  private def toRejection(ex: EntityStreamSizeException): InstanceRejection =
    AttachmentLimitExceeded(ex.limit)

  /**
    * The discriminator is enough to give us a Json representation (the name of the class)
    */
  private implicit val config: Configuration = Configuration.default.withDiscriminator("code")

  private val context = Json.obj(`@context` -> Json.fromString(errorContext.toString))

  private implicit val instanceRejectionEncoder: Encoder[InstanceRejection] =
    deriveEncoder[InstanceRejection].mapJson(_.deepMerge(context))

  private implicit val schemaRejectionEncoder: Encoder[SchemaRejection] =
    deriveEncoder[SchemaRejection].mapJson(_.deepMerge(context))

  private implicit val contextRejectionEncoder: Encoder[ContextRejection] =
    deriveEncoder[ContextRejection].mapJson(_.deepMerge(context))

  private implicit val domainRejectionEncoder: Encoder[DomainRejection] =
    deriveEncoder[DomainRejection].mapJson(_.deepMerge(context))

  private implicit val orgRejectionEncoder: Encoder[OrgRejection] =
    deriveEncoder[OrgRejection].mapJson(_.deepMerge(context))

  private implicit val illegalFilterFormatEncoder: Encoder[IllegalFilterFormat] =
    deriveEncoder[IllegalFilterFormat].mapJson(_.deepMerge(context))

  private implicit val internalErrorEncoder: Encoder[InternalError] =
    deriveEncoder[InternalError].mapJson(_.deepMerge(context))

  private implicit val instanceStatusFrom: StatusFrom[InstanceRejection] = {
    import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRejection._
    StatusFrom {
      case IncorrectRevisionProvided    => Conflict
      case InstanceAlreadyExists        => Conflict
      case InstanceDoesNotExist         => NotFound
      case AttachmentNotFound           => NotFound
      case _: AttachmentLimitExceeded   => BadRequest
      case InstanceIsDeprecated         => BadRequest
      case _: ShapeConstraintViolations => BadRequest
    }
  }

  private implicit val schemaStatusFrom: StatusFrom[SchemaRejection] = {
    import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection._
    StatusFrom {
      case SchemaAlreadyExists          => Conflict
      case IncorrectRevisionProvided    => Conflict
      case SchemaDoesNotExist           => NotFound
      case CannotUnpublishSchema        => BadRequest
      case CannotUpdatePublished        => BadRequest
      case SchemaIsDeprecated           => BadRequest
      case SchemaIsNotPublished         => BadRequest
      case _: ShapeConstraintViolations => BadRequest
      case _: MissingImportsViolation   => BadRequest
      case _: IllegalImportsViolation   => BadRequest
      case _: InvalidSchemaId           => BadRequest
    }
  }

  private implicit val contextStatusFrom: StatusFrom[ContextRejection] = {
    import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRejection._
    StatusFrom {
      case ContextAlreadyExists         => Conflict
      case IncorrectRevisionProvided    => Conflict
      case ContextDoesNotExist          => NotFound
      case CannotUnpublishContext       => BadRequest
      case CannotUpdatePublished        => BadRequest
      case ContextIsDeprecated          => BadRequest
      case ContextIsNotPublished        => BadRequest
      case _: ShapeConstraintViolations => BadRequest
      case _: IllegalImportsViolation   => BadRequest
      case _: InvalidContextId          => BadRequest
    }
  }

  private implicit val domainStatusFrom: StatusFrom[DomainRejection] = {
    import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection._
    StatusFrom {
      case DomainAlreadyExists       => Conflict
      case IncorrectRevisionProvided => Conflict
      case DomainDoesNotExist        => NotFound
      case DomainIsDeprecated        => BadRequest
      case _: InvalidDomainId        => BadRequest
      case DomainAlreadyDeprecated   => BadRequest
    }
  }

  private implicit val orgStatusFrom: StatusFrom[OrgRejection] = {
    import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection._
    StatusFrom {
      case OrgAlreadyExists          => Conflict
      case IncorrectRevisionProvided => Conflict
      case OrgDoesNotExist           => NotFound
      case OrgIsDeprecated           => BadRequest
      case _: InvalidOrganizationId  => BadRequest
    }
  }

  private implicit val filterStatusFrom: StatusFrom[IllegalFilterFormat] =
    StatusFrom(_ => BadRequest)

  private implicit val internalErrorStatusFrom: StatusFrom[InternalError] =
    StatusFrom(_ => InternalServerError)

  /**
    * An internal error representation that can safely be returned in its json form to the caller.
    *
    * @param code the code displayed as a response (InternalServerError as default)
    */
  private case class InternalError(code: String = "InternalServerError")

}

object ExceptionHandling {

  /**
    * @param errorContext the context URI to be injected in the JSON-LD error responses
    * @return an ExceptionHandler for [[ch.epfl.bluebrain.nexus.kg.core.Fault]] subtypes that ensures a descriptive
    *         message is returned to the caller
    */
  final def exceptionHandler(errorContext: Uri): ExceptionHandler = {
    val handler = new ExceptionHandling(errorContext)
    handler.exceptionHandler
  }
}
