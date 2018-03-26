package ch.epfl.bluebrain.nexus.kg.core.schemas

import cats.MonadError
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclSchema
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidatorErr.CouldNotFindImports
import ch.epfl.bluebrain.nexus.kg.core.BaseImportResolver
import io.circe.Json

import scala.util.Try

/**
  * A transitive ''ImportResolver'' implementation that ensures schema imports are known uris within a published state.
  *
  * @param baseUri         the base uri of the system
  * @param schemaLoader    function that allows dynamic schema lookup in the system
  * @param contextResolver function that allows dynamic context resolution
  * @param F               a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
class SchemaImportResolver[F[_]](baseUri: String,
                                 schemaLoader: SchemaId => F[Option[Schema]],
                                 contextResolver: Json => F[Json])(implicit F: MonadError[F, Throwable])
    extends BaseImportResolver[F, SchemaId, Schema](schemaLoader, contextResolver) {

  override def idBaseUri: String = s"$baseUri/schemas"

  override def toId(str: String): Try[SchemaId] = SchemaId(str)

  override def asJson(resource: Schema): Json = resource.value

  override def addJson(resource: Schema, json: Json): Schema = resource.copy(value = json)

  override def checkImports(
      list: List[(SchemaId, Option[Schema])]): Either[CouldNotFindImports, List[(SchemaId, ShaclSchema)]] = {
    list.foldLeft[Either[CouldNotFindImports, List[(SchemaId, ShaclSchema)]]](Right(Nil)) {
      case (Left(CouldNotFindImports(missing)), (id, None)) => Left(CouldNotFindImports(missing + qualify(id)))
      case (Left(CouldNotFindImports(missing)), (id, Some(sch))) if !sch.published =>
        Left(CouldNotFindImports(missing + qualify(id)))
      case (l @ Left(_), _)                              => l
      case (Right(_), (id, None))                        => Left(CouldNotFindImports(Set(qualify(id))))
      case (Right(_), (id, Some(sch))) if !sch.published => Left(CouldNotFindImports(Set(qualify(id))))
      case (Right(acc), (id, Some(sch)))                 => Right(id -> toShaclSchema(id, sch) :: acc)
    }
  }

}
