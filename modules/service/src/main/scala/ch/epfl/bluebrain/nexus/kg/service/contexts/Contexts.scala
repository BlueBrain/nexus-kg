package ch.epfl.bluebrain.nexus.kg.service.contexts

import cats.MonadError
import ch.epfl.bluebrain.nexus.kg.service.contexts.Contexts._
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.Agg
import ch.epfl.bluebrain.nexus.kg.service.operations.{Operations, ResourceState}
import com.github.ghik.silencer.silent
import io.circe.Json
import journal.Logger

class Contexts[F[_]](agg: Agg[F, ContextId])(implicit F: MonadError[F, Throwable])
    extends Operations[F, ContextId, Json](agg, logger) {

  override type Resource = Context

  override implicit def buildResource(c: ResourceState.Current[ContextId, Json]): Resource =
    Context(c.id, c.rev, c.value, c.deprecated)

  @silent
  override def validate(id: ContextId, value: Json): F[Unit] = F.pure(())
}

object Contexts {

  final def apply[F[_]](agg: Agg[F, ContextId])(implicit F: MonadError[F, Throwable]): Contexts[F] =
    new Contexts[F](agg)

  private[contexts] val logger = Logger[this.type]

  def next(state: ResourceState, event: Operations.ResourceEvent[ContextId]): ResourceState =
    ResourceState.next[ContextId, Json](state, event)

  def eval(state: ResourceState, cmd: Operations.ResourceCommand[ContextId])
    : Either[Operations.ResourceRejection, Operations.ResourceEvent[ContextId]] =
    ResourceState.eval[ContextId, Json](state, cmd)

}