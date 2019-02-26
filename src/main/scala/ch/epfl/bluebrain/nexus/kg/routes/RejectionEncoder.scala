package ch.epfl.bluebrain.nexus.kg.routes

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidJsonLD, UnableToSelectResourceId}
import ch.epfl.bluebrain.nexus.kg.resources.{Rejection, Resource, ResourceV}
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat.Compacted
import ch.epfl.bluebrain.nexus.rdf.MarshallingError
import ch.epfl.bluebrain.nexus.rdf.MarshallingError.{ConversionError, RootNodeNotFound, Unexpected}
import io.circe.Encoder

/**
  * An encoder that can reject
  *
  * @tparam A the generic type
  */
sealed trait RejectionEncoder[A] {

  /**
    * An evaluation of the provided value produces either a Rejection or an Encoder[A]
    * @param value the provided value to evaluate
    */
  def apply(value: A): Either[Rejection, Encoder[A]]
}

object RejectionEncoder {

  final implicit def rejectionEncoder(
      implicit outputFormat: JsonLDOutputFormat = Compacted): RejectionEncoder[ResourceV] =
    new RejectionEncoder[ResourceV] {
      override def apply(value: ResourceV): Either[Rejection, Encoder[ResourceV]] =
        ResourceEncoder
          .json(value)
          .map(json => Encoder.instance((_: ResourceV) => json))
          .left
          .map(marshallerErrorToRejectiton)
    }

  final implicit def rejectionEncoder(implicit config: AppConfig, project: Project): RejectionEncoder[Resource] =
    new RejectionEncoder[Resource] {
      override def apply(value: Resource): Either[Rejection, Encoder[Resource]] =
        ResourceEncoder
          .json(value)
          .map(json => Encoder.instance((_: Resource) => json))
          .left
          .map(marshallerErrorToRejectiton)
    }
  private def marshallerErrorToRejectiton(err: MarshallingError): Rejection = err match {
    case ConversionError(message, _) => InvalidJsonLD(message)
    case Unexpected(message)         => InvalidJsonLD(message)
    case RootNodeNotFound(_)         => UnableToSelectResourceId
  }

}
