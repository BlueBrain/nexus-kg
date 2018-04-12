package ch.epfl.bluebrain.nexus.kg.core.resources

import io.circe.{Decoder, Encoder, Json}

/**
  * The unique key which identifies a representation of a resource
  *
  * @param projectRef the identifier for the project. TODO: Change the type to a [ProjectReference]
  * @param resourceId the unique identifier (inside this project) of the current resource. TODO: Change the type to a [IRI] or [Id]
  * @param schemaId   the unique identifier (inside this project) of the schema which constrains this resource. TODO: Change the type to a [IRI] or [Id]
  */
final case class Key(projectRef: String, resourceId: String, schemaId: String)
object Key {

  implicit val encodeKey: Encoder[Key] = Encoder.encodeJson.contramap {
    case Key(project, resource, schema) =>
      Json.obj("@id"     -> Json.fromString(resource),
               "project" -> Json.fromString(project),
               "schema"  -> Json.fromString(schema))
  }

  implicit val decodeKey: Decoder[Key] = Decoder.forProduct3("project", "@id", "schema")(Key.apply)
}
