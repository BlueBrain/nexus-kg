package ch.epfl.bluebrain.nexus.kg.service.refs

trait Ref[Id] {
  def id: Id
}

object Ref {
  // $COVERAGE-OFF$
  private final case class Const[Id](id: Id) extends Ref[Id]
  final def apply[Id](id: Id): Ref[Id] = Const(id)
  // $COVERAGE-ON$
}
