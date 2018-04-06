package ch.epfl.bluebrain.nexus.kg.core.instances

import cats.MonadError
import ch.epfl.bluebrain.nexus.kg.core.BaseImportResolver
import io.circe.Json

import scala.util.Try

/**
  * A transitive ''ImportResolver'' implementation that ensures instance imports are known uris.
  *
  * @param baseUri         the base uri of the system
  * @param instanceLoader  function that allows dynamic instance lookup in the system
  * @param contextResolver function that allows dynamic context resolution
  * @param F               a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
class InstanceImportResolver[F[_]](baseUri: String,
                                   instanceLoader: InstanceId => F[Option[Instance]],
                                   contextResolver: Json => F[Json])(implicit F: MonadError[F, Throwable])
    extends BaseImportResolver[F, InstanceId, Instance](instanceLoader, contextResolver) {

  override val idBaseUri = s"$baseUri/data"

  override def idBaseUriToIgnore: Set[String] = Set(s"$baseUri/schemas/")

  override def toId(str: String): Try[InstanceId] = InstanceId.apply(str)

  override def asJson(resource: Instance): Json = resource.value

  override def addJson(resource: Instance, json: Json): Instance = resource.copy(value = json)
}
