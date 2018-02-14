package ch.epfl.bluebrain.nexus.kg.service.operations

import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceCommand.{CreateResource, DeprecateResource, UpdateResource}
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceEvent.{ResourceCreated, ResourceDeprecated, ResourceUpdated}
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceRejection._
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.{ResourceCommand, ResourceEvent, ResourceRejection}
import shapeless.Typeable

sealed trait ResourceState extends Product with Serializable

object ResourceState {

  final case object Initial                                                                     extends ResourceState
  final case class Current[Id, V](id: Id, rev: Long, meta: Meta, value: V, deprecated: Boolean) extends ResourceState

  def cast[Id: Typeable, V: Typeable](c: Current[_, _]): Option[Current[Id, V]] =
    cast[Id, V](c.id, c.value) map { case (id, value) => c.copy(id = id, value = value) }

  private def cast[Id, V](id: Any, value: Any)(implicit Id: Typeable[Id], V: Typeable[V]): Option[(Id, V)] =
    for {
      id    <- Id.cast(id)
      value <- V.cast(value)
    } yield (id -> value)

  def next[Id: Typeable, V: Typeable](state: ResourceState, event: ResourceEvent[Id]): ResourceState =
    (state, event) match {
      case (Initial, ResourceCreated(id, rev, meta, value)) =>
        Current(id, rev, meta, value, deprecated = false)
      // $COVERAGE-OFF$
      case (Initial, _) => Initial
      // $COVERAGE-ON$
      case (c @ Current(_, _, _, _, deprecated), _) if deprecated => c
      case (c, _: ResourceCreated[_, _])                          => c
      case (c: Current[_, _], ResourceUpdated(_, rev, meta, value)) =>
        cast[Id, V](c).map(casted => casted.copy(rev = rev, meta = meta, value = value)).getOrElse(state)
      case (c: Current[_, _], ResourceDeprecated(_, rev, meta)) =>
        cast[Id, V](c).map(casted => casted.copy(rev = rev, meta = meta, deprecated = true)).getOrElse(state)
    }

  def eval[Id: Typeable, V: Typeable](state: ResourceState,
                                      cmd: ResourceCommand[Id]): Either[ResourceRejection, ResourceEvent[Id]] = {

    def createResource(c: CreateResource[_, _]): Either[ResourceRejection, ResourceEvent[Id]] = state match {
      case Initial =>
        cast[Id, V](c.id, c.value)
          .map[ResourceEvent[Id]] { case (id, value) => ResourceCreated(id, 1L, c.meta, value) }
          .toRight(UnexpectedCasting)
      case _ => Left(ResourceAlreadyExists)
    }

    def updateResource(c: UpdateResource[_, _]): Either[ResourceRejection, ResourceEvent[Id]] = state match {
      case Initial                                  => Left(ResourceDoesNotExists)
      case Current(_, rev, _, _, _) if rev != c.rev => Left(IncorrectRevisionProvided)
      case Current(_, _, _, _, true)                => Left(ResourceIsDeprecated)
      case s: Current[_, _] =>
        cast[Id, V](s)
          .map(casted => ResourceUpdated(casted.id, casted.rev + 1, c.meta, c.value))
          .toRight(UnexpectedCasting)
    }

    def deprecateResource(c: DeprecateResource[_, _]): Either[ResourceRejection, ResourceEvent[Id]] = state match {
      case Initial                                  => Left(ResourceDoesNotExists)
      case Current(_, rev, _, _, _) if rev != c.rev => Left(IncorrectRevisionProvided)
      case Current(_, _, _, _, true)                => Left(ResourceIsDeprecated)
      case s: Current[_, _] =>
        cast[Id, V](s).map(casted => ResourceDeprecated(casted.id, casted.rev + 1, c.meta)).toRight(UnexpectedCasting)
    }

    cmd match {
      case c: CreateResource[_, _]    => createResource(c)
      case c: UpdateResource[_, _]    => updateResource(c)
      case c: DeprecateResource[_, _] => deprecateResource(c)
    }
  }
}
