package ch.epfl.bluebrain.nexus.kg.service.serialization

import ch.epfl.bluebrain.nexus.commons.service.io.AkkaCoproductSerializer
import ch.epfl.bluebrain.nexus.kg.service.contexts.ContextEvent
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectEvent
import ch.epfl.bluebrain.nexus.kg.service.schemas.SchemaEvent
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.java8.time._
import shapeless.{:+:, CNil}

/**
  * Akka ''SerializerWithStringManifest'' class definition for all events.
  * The serializer provides the types available for serialization.
  */
object Serializer {

  private implicit val config: Configuration = Configuration.default.withDiscriminator("type")

  class EventSerializer extends AkkaCoproductSerializer[SchemaEvent :+: ContextEvent :+: ProjectEvent :+: CNil](1215)

}
