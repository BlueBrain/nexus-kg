package ch.epfl.bluebrain.nexus.kg.service.projects

import cats.MonadError
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig.OperationsConfig
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.Agg
import ch.epfl.bluebrain.nexus.kg.service.operations.{Operations, ResourceState}
import ch.epfl.bluebrain.nexus.kg.service.projects.Projects._
import io.circe.Json
import journal.Logger

class Projects[F[_]](agg: Agg[F, ProjectId])(implicit F: MonadError[F, Throwable], config: OperationsConfig)
    extends Operations[F, ProjectId, Value](agg, logger) {

  override type Resource = Project

  override implicit def buildResource(c: ResourceState.Current[ProjectId, Value]): Resource =
    Project(c.id, c.rev, c.value.context, c.value.config, c.deprecated)
}

object Projects {
  final def apply[F[_]](agg: Agg[F, ProjectId])(implicit F: MonadError[F, Throwable],
                                                config: OperationsConfig): Projects[F] =
    new Projects[F](agg)

  private[projects] val logger = Logger[this.type]
  final case class Value(context: Json, config: Project.Config)

  def next(state: ResourceState, event: Operations.ResourceEvent[ProjectId]): ResourceState =
    ResourceState.next[ProjectId, Json](state, event)

  def eval(state: ResourceState, cmd: Operations.ResourceCommand[ProjectId])
    : Either[Operations.ResourceRejection, Operations.ResourceEvent[ProjectId]] =
    ResourceState.eval[ProjectId, Json](state, cmd)

}
