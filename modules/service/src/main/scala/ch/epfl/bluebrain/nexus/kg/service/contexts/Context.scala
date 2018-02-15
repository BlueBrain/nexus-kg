package ch.epfl.bluebrain.nexus.kg.service.contexts

import io.circe.Json

final case class Context(id: ContextId, rev: Long, value: Json, deprecated: Boolean)