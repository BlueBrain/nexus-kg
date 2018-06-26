package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources
import io.circe.Json

object Contexts extends Resources {
  val tags: Json = jsonContentOf("/contexts/resource-context.json")

}
