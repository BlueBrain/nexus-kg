package ch.epfl.bluebrain.nexus.kg.service.io

import ch.epfl.bluebrain.nexus.kg.core.domains._
import ch.epfl.bluebrain.nexus.kg.core.instances._
import ch.epfl.bluebrain.nexus.kg.core.organizations._
import ch.epfl.bluebrain.nexus.kg.core.schemas._
import ch.epfl.bluebrain.nexus.commons.service.io.AkkaCoproductSerializer
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent
import io.circe.java8.time._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import shapeless._

/**
  * Akka ''SerializerWithStringManifest'' class definition for all events.
  * The serializer provides the types available for serialization.
  */
object Serializer {

  implicit val config: Configuration =
    Configuration.default.withDiscriminator("type")

  class EventSerializer
      extends AkkaCoproductSerializer[
        InstanceEvent :+: SchemaEvent :+: ContextEvent :+: DomainEvent :+: OrgEvent :+: CNil](1215)

}
