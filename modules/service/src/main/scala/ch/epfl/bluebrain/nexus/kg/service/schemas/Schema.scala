package ch.epfl.bluebrain.nexus.kg.service.schemas

import io.circe.Json

final case class Schema(id: SchemaId, rev: Long, value: Json, deprecated: Boolean)