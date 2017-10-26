package ch.epfl.bluebrain.nexus.kg.core.contexts

import ch.epfl.bluebrain.nexus.kg.core.Ref

/**
  * Reference data type to a specific context revision.
  *
  * @param id  the unique identifier for the context
  * @param rev a revision identifier for the context
  */
final case class ContextRef(id: ContextId, rev: Long)

object ContextRef {
  final implicit def contextRef(ref: ContextRef): Ref[ContextId] = Ref(ref.id, ref.rev)
}