package ch.epfl.bluebrain.nexus.kg.service.refs

import ch.epfl.bluebrain.nexus.kg.service.types.Revisioned

trait RevisionedRef[Id] extends Ref[Id] with Revisioned

object RevisionedRef {
  private final case class Const[Id](id: Id, rev: Long) extends RevisionedRef[Id]
  final def apply[Id](id: Id, rev: Long): RevisionedRef[Id] = Const(id, rev)
}
