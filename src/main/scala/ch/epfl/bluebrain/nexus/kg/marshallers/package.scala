package ch.epfl.bluebrain.nexus.kg

import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidPayload
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveEncoder
import io.circe.parser.parse
import io.circe.{Encoder, Json}

package object marshallers {

  private implicit def aEncoder[A: Show]: Encoder[A] = Encoder.encodeString.contramap(_.show)

  /**
    * The discriminator is enough to give us a Json representation (the name of the class)
    */
  private[marshallers] implicit val rejectionConfig: Configuration = Configuration.default.withDiscriminator("code")

  private[marshallers] implicit val rejectionEncoder: Encoder[Rejection] = {
    val enc = deriveEncoder[Rejection].mapJson(_ addContext errorCtxUri)
    Encoder.instance {
      case r: InvalidPayload => enc(r) deepMerge jsonMsg(r) deepMerge jsonReason(r.reason)
      case r                 => enc(r) deepMerge jsonMsg(r)
    }
  }

  private def jsonMsg(r: Rejection): Json = Json.obj("message" -> Json.fromString(r.msg))

  private def jsonReason(reason: String): Json = parse(reason).getOrElse(Json.fromString(reason))

  private[marshallers] implicit val httpRejectionEncoder: Encoder[HttpRejection] =
    deriveEncoder[HttpRejection].mapJson(_ addContext errorCtxUri)

  val sparqlQueryUnmarshaller: FromEntityUnmarshaller[String] = PredefinedFromEntityUnmarshallers.stringUnmarshaller
    .forContentTypes(RdfMediaTypes.`application/sparql-query`, MediaTypes.`text/plain`)
}
