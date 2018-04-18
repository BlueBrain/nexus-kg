package ch.epfl.bluebrain.nexus.kg.core.resources

import akka.http.scaladsl.model.Uri.Path
import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AdminConfig
import io.circe.{Decoder, Encoder, Json}
import ch.epfl.bluebrain.nexus.kg.core.UriOps._

/**
  * The unique id which identifies a representation of a resource
  *
  * @param projectRef the identifier for the project. TODO: Change the type to a [ProjectReference]
  * @param resourceId the unique identifier (inside this project) of the current resource. TODO: Change the type to a [IRI] or [Id]
  * @param schemaId   the unique identifier (inside this project) of the schema which constrains this resource. TODO: Change the type to a [IRI] or [Id]
  */
final case class RepresentationId(projectRef: String, resourceId: String, schemaId: String) {
  def persId: String = s"${projectRef}_${resourceId}_${schemaId}"
}
object RepresentationId {

  implicit def encodeReprId(implicit config: AdminConfig): Encoder[RepresentationId] = Encoder.encodeJson.contramap {
    case RepresentationId(projectRef, resource, schema) =>
      Json.obj(
        "@id"     -> Json.fromString(resource),
        "project" -> Json.fromString(config.projectUri.append(Path(projectRef)).toString()),
        "schema"  -> Json.fromString(schema)
      )
  }

  implicit def decodeReprId(implicit config: AdminConfig): Decoder[RepresentationId] =
    Decoder.forProduct3("project", "@id", "schema")(RepresentationId.apply).map {
      case RepresentationId(uri, id, schemaId) =>
        val projectRef = uri.stripPrefix(config.projectUri.toString()).stripPrefix("/")
        RepresentationId(projectRef, id, schemaId)
    }
}
