package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server._
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._

/**
  * A rejection encapsulates a specific reason why a route was not able to handle a request.
  * Rejections are gathered up over the course of a Route evaluation and finally
  * converted to CommonRejections case classes if there was no way for the request to be completed.
  */
object RejectionHandling {

  /**
    * Defines the custom handling of rejections. When multiple rejections are generated
    * in the routes evaluation process, the priority order to handle them is defined
    * by the order of appearance in this method.
    */
  final def rejectionHandler: RejectionHandler =
    RejectionHandler
      .newBuilder()
      .handle {
        case MalformedQueryParamRejection(_, _, Some(e: WrongOrInvalidJson)) =>
          complete(BadRequest -> (e: CommonRejections))
        case MalformedQueryParamRejection(_, _, Some(e: IllegalFilterFormat)) =>
          complete(BadRequest -> (e: CommonRejections))
        case ValidationRejection(_, Some(e: IllegalVersionFormat)) =>
          complete(BadRequest -> (e: CommonRejections))
      }
      .handleAll[MalformedRequestContentRejection] { rejection =>
        val aggregate = rejection.map(_.message).mkString(", ")
        complete(BadRequest -> (WrongOrInvalidJson(Some(aggregate)): CommonRejections))
      }
      .handleAll[MethodRejection] { methodRejections =>
        val names = methodRejections.map(_.supported.name)
        complete(MethodNotAllowed -> (MethodNotSupported(names): CommonRejections))
      }
      .result()

  /**
    * The discriminator is enough to give us a Json representation (the name of the class)
    */
  private implicit val rejectionConfig: Configuration = Configuration.default.withDiscriminator("code")

}
