package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.{Applicative, Monad, MonadError}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types.{FullAccessControlList, Identity}
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{AggregateElasticView, ElasticView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder.viewGraphEncoder
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder.resolverGraphEncoder
import ch.epfl.bluebrain.nexus.kg.resources.AdditionalValidation._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidIdentity, InvalidPayload}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._

trait AdditionalValidation[F[_]] {

  /**
    * Performs additional validation
    *
    * @param id     the unique identifier of the resource
    * @param schema the schema that this resource conforms to
    * @param types  the collection of known types of this resource
    * @param value  the resource value
    * @param rev    the last known revision of the resource + 1
    * @return a Left(rejection) when the validation does not pass or Right(value) when it does on the effect type ''F''
    */
  def apply(id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value, rev: Long): EitherT[F, Rejection, Value]
}

object AdditionalValidation {

  type Value = ResourceF.Value

  /**
    * @tparam F the monadic effect type
    * @return a new validation that always returns Right(value) on the provided effect type
    */
  final def pass[F[_]: Applicative]: AdditionalValidation[F] =
    (_: ResId, _: Ref, _: Set[AbsoluteIri], value: Value, _: Long) => EitherT.rightT(value)

  /**
    * Additional validation used for checking the correctness of the ElasticSearch mappings
    *
    * @tparam F the monadic effect type
    * @return a new validation that passes whenever the provided mappings are compliant with the ElasticSearch mappings or
    *         when the view is not an ElasticView
    */
  final def view[F[_]](caller: Caller, acls: FullAccessControlList)(
      implicit F: MonadError[F, Throwable],
      elastic: ElasticClient[F],
      config: ElasticConfig,
      cache: DistributedCache[F]): AdditionalValidation[F] =
    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value, rev: Long) => {
      val resource = ResourceF.simpleV(id, value, rev = rev, types = types, schema = schema)
      EitherT.fromEither(View(resource)).flatMap {
        case es: ElasticView => EitherT(es.createIndex[F]).map(_ => value)
        case agg: AggregateElasticView[_] =>
          agg.referenced[F](caller, acls).map(r => value.map(r, _.removeKeys("@context").addContext(viewCtxUri)))
        case _ => EitherT.rightT(value)
      }
    }

  /**
    * Additional validation used for checking ACLs on [[Resolver]] creation
    *
    * @param caller     the [[Caller]] with all it's identities
    * @param accountRef the account reference
    * @tparam F the monadic effect type
    * @return a new validation that passes whenever the provided ''acls'' match the ones on the resolver's identities
    */
  final def resolver[F[_]](caller: Caller, accountRef: AccountRef)(
      implicit F: Monad[F],
      cache: DistributedCache[F]): AdditionalValidation[F] = {

    def aclContains(identities: List[Identity]): Boolean =
      identities.forall(caller.identities.contains)

    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value, rev: Long) =>
      {
        val resource = ResourceF.simpleV(id, value, rev = rev, types = types, schema = schema)
        Resolver(resource, accountRef) match {
          case Some(resolver: CrossProjectResolver[_]) if aclContains(resolver.identities) =>
            resolver.referenced.map(r => value.map(r, _.removeKeys("@context").addContext(resolverCtxUri)))
          case Some(resolver: InAccountResolver) if aclContains(resolver.identities) =>
            EitherT.rightT[F, Rejection](value)
          case Some(_: InProjectResolver) => EitherT.rightT[F, Rejection](value)
          case Some(_) =>
            EitherT.leftT[F, Value](
              InvalidIdentity("Your account does not contain all the identities provided"): Rejection)
          case None =>
            EitherT.leftT[F, Value](
              InvalidPayload(id.ref, "The provided payload could not be mapped to a Resolver"): Rejection)
        }
      }
  }
}
