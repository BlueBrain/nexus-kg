package ch.epfl.bluebrain.nexus.kg.service.operations

trait RevisionedRef[Id] extends Ref[Id] {
  def rev: Long
}

object RevisionedRef {
  private final case class Const[Id](id: Id, rev: Long) extends RevisionedRef[Id]
  final def apply[Id](id: Id, rev: Long): RevisionedRef[Id] = Const(id, rev)
}
