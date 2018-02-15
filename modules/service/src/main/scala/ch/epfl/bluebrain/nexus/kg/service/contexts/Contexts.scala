package ch.epfl.bluebrain.nexus.kg.service.contexts

import cats.MonadError
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig.OperationsConfig
import ch.epfl.bluebrain.nexus.kg.service.contexts.Contexts._
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.Agg
import ch.epfl.bluebrain.nexus.kg.service.operations.{Operations, ResourceState}
import ch.epfl.bluebrain.nexus.kg.service.projects.Projects
import io.circe.Json
import journal.Logger

class Contexts[F[_]](agg: Agg[F, ContextId, ContextEvent], projects: Projects[F])(implicit F: MonadError[F, Throwable],
                                                                                  config: OperationsConfig)
    extends Operations[F, ContextId, Json, ContextEvent](agg, logger) {

  override type Resource = Context

  override def validate(id: ContextId, value: Json): F[Unit] =
    super.validate(id, value) product projects.validateUnlocked(id.projectId) map (_ => ())

  override implicit def buildResource(c: ResourceState.Current[ContextId, Json]): Resource =
    Context(c.id, c.rev, c.value, c.deprecated)
}

object Contexts {

  final def apply[F[_]](agg: Agg[F, ContextId, ContextEvent], projects: Projects[F])(
      implicit F: MonadError[F, Throwable],
      config: OperationsConfig): Contexts[F] = new Contexts[F](agg, projects)

  private[contexts] val logger = Logger[this.type]

  def next(state: ResourceState, event: ContextEvent): ResourceState =
    ResourceState.next[ContextId, Json, ContextEvent](state, event)

  def eval(state: ResourceState,
           cmd: Operations.ResourceCommand[ContextId]): Either[Operations.ResourceRejection, ContextEvent] =
    ResourceState.eval[ContextId, Json, ContextEvent](state, cmd)

}
