package ch.epfl.bluebrain.nexus.kg.service.schemas

import cats.MonadError
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig.OperationsConfig
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.Agg
import ch.epfl.bluebrain.nexus.kg.service.operations.{Operations, ResourceState}
import ch.epfl.bluebrain.nexus.kg.service.projects.Projects
import ch.epfl.bluebrain.nexus.kg.service.schemas.Schemas._
import io.circe.Json
import journal.Logger

class Schemas[F[_]](agg: Agg[F, SchemaId, SchemaEvent], projects: Projects[F])(implicit F: MonadError[F, Throwable],
                                                                               config: OperationsConfig)
    extends Operations[F, SchemaId, Json, SchemaEvent](agg, logger) {

  override type Resource = Schema

  override def validate(id: SchemaId, value: Json): F[Unit] =
    super.validate(id, value) product projects.validateUnlocked(id.projectId) map (_ => ())

  override implicit def buildResource(c: ResourceState.Current[SchemaId, Json]): Resource =
    Schema(c.id, c.rev, c.value, c.deprecated)
}

object Schemas {

  final def apply[F[_]](agg: Agg[F, SchemaId, SchemaEvent], projects: Projects[F])(
      implicit F: MonadError[F, Throwable],
      config: OperationsConfig): Schemas[F] = new Schemas[F](agg, projects)

  private[schemas] val logger = Logger[this.type]

  def next(state: ResourceState, event: SchemaEvent): ResourceState =
    ResourceState.next[SchemaId, Json, SchemaEvent](state, event)

  def eval(state: ResourceState,
           cmd: Operations.ResourceCommand[SchemaId]): Either[Operations.ResourceRejection, SchemaEvent] =
    ResourceState.eval[SchemaId, Json, SchemaEvent](state, cmd)

}
