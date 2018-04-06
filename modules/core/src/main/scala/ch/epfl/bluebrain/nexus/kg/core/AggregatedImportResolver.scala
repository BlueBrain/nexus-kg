package ch.epfl.bluebrain.nexus.kg.core

import cats.MonadError
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidatorErr.IllegalImportDefinition
import ch.epfl.bluebrain.nexus.commons.shacl.validator.{ImportResolver, ShaclSchema}
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceImportResolver
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaImportResolver

/**
  * A transitive ''ImportResolver'' implementation that aggregates an instances import resolver and a schema import resolver.
  *
  * @param schemaImportResolver   the transitive implementation of a ''ImportResolver'' for schemas
  * @param instanceImportResolver the transitive implementation of a ''ImportResolver'' for instances
  * @param F                      a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
class AggregatedImportResolver[F[_]](
    schemaImportResolver: SchemaImportResolver[F],
    instanceImportResolver: InstanceImportResolver[F])(implicit F: MonadError[F, Throwable])
    extends ImportResolver[F] {

  override def apply(schema: ShaclSchema): F[Set[ShaclSchema]] = {
    val instanceResolved = instanceImportResolver(schema)
    schemaImportResolver(schema)
      .recoverWith {
        case _: IllegalImportDefinition => instanceResolved
        case err                        => F.raiseError(err)
      }
      .flatMap(schemaSet => instanceResolved.map(_ ++ schemaSet))
  }
}

object AggregatedImportResolver {

  final def apply[F[_]](schemaImportResolver: SchemaImportResolver[F],
                        instanceImportResolver: InstanceImportResolver[F])(
      implicit F: MonadError[F, Throwable]): AggregatedImportResolver[F] =
    new AggregatedImportResolver(schemaImportResolver, instanceImportResolver)
}
