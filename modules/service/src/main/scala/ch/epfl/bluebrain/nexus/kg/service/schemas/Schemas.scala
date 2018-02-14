package ch.epfl.bluebrain.nexus.kg.service.schemas

import cats.MonadError
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.Agg
import ch.epfl.bluebrain.nexus.kg.service.operations.{Operations, ResourceState}
import ch.epfl.bluebrain.nexus.kg.service.schemas.Schemas._
import com.github.ghik.silencer.silent
import io.circe.Json
import journal.Logger

class Schemas[F[_]](agg: Agg[F, SchemaId])(implicit F: MonadError[F, Throwable])
    extends Operations[F, SchemaId, Json](agg, logger) {

  override type Resource = Schema

  override implicit def buildResource(c: ResourceState.Current[SchemaId, Json]): Resource =
    Schema(c.id, c.rev, c.value, c.deprecated)

  @silent
  override def validate(id: SchemaId, value: Json): F[Unit] = F.pure(())
}

object Schemas {

  final def apply[F[_]](agg: Agg[F, SchemaId])(implicit F: MonadError[F, Throwable]): Schemas[F] = new Schemas[F](agg)

  private[schemas] val logger = Logger[this.type]

  def next(state: ResourceState, event: Operations.ResourceEvent[SchemaId]): ResourceState =
    ResourceState.next[SchemaId, Json](state, event)

  def eval(state: ResourceState, cmd: Operations.ResourceCommand[SchemaId])
    : Either[Operations.ResourceRejection, Operations.ResourceEvent[SchemaId]] =
    ResourceState.eval[SchemaId, Json](state, cmd)

}
