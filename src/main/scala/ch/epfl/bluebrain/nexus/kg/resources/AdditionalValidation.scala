package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.syntax.all._
import cats.{Applicative, Monad, MonadError}
import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticSearchConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{AggregateElasticSearchView, ElasticSearchView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.AdditionalValidation._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidIdentity, InvalidJsonLD, InvalidResourceFormat}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._

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
    *         when the view is not an ElasticSearchView
    */
  final def view[F[_]](caller: Caller, acls: AccessControlLists)(implicit F: MonadError[F, Throwable],
                                                                 elasticSearch: ElasticSearchClient[F],
                                                                 config: ElasticSearchConfig,
                                                                 projectCache: ProjectCache[F],
                                                                 viewCache: ViewCache[F]): AdditionalValidation[F] =
    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value, rev: Long) => {
      val resource = ResourceF.simpleV(id, value, rev = rev, types = types, schema = schema)
      EitherT.fromEither(View(resource)).flatMap {
        case es: ElasticSearchView =>
          EitherT(
            es.createIndex[F]
              .map[Either[Rejection, Value]](_ => Right(value))
              .recoverWith {
                case ElasticClientError(_, body) => F.pure(Left(InvalidResourceFormat(id.ref, body)))
              }
          )
        case agg: AggregateElasticSearchView[_] =>
          agg.referenced[F](caller, acls).flatMap { r =>
            EitherT.fromOption[F](value.map(r, _.removeKeys("@context").addContext(viewCtxUri)),
                                  InvalidJsonLD("Could not convert the graph to Json"))
          }
        case _ => EitherT.rightT(value)
      }
    }

  /**
    * Additional validation used for checking ACLs on [[Resolver]] creation
    *
    * @param caller     the [[Caller]] with all it's identities
    * @tparam F the monadic effect type
    * @return a new validation that passes whenever the provided ''acls'' match the ones on the resolver's identities
    */
  final def resolver[F[_]](caller: Caller)(implicit F: Monad[F],
                                           projectCache: ProjectCache[F]): AdditionalValidation[F] = {

    def aclContains(identities: List[Identity]): Boolean =
      identities.forall(caller.identities.contains)

    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value, rev: Long) =>
      {
        val resource = ResourceF.simpleV(id, value, rev = rev, types = types, schema = schema)
        Resolver(resource) match {
          case Some(resolver: CrossProjectResolver[_]) if aclContains(resolver.identities) =>
            resolver.referenced.flatMap { r =>
              EitherT.fromOption[F](value.map(r, _.removeKeys("@context").addContext(resolverCtxUri)),
                                    InvalidJsonLD("Could not convert the graph to Json"))
            }
          case Some(_: CrossProjectResolver[_]) =>
            EitherT.leftT[F, Value](
              InvalidIdentity("Your user doesn't have some of the provided identities on the resolver"): Rejection)
          case Some(_: InProjectResolver) => EitherT.rightT[F, Rejection](value)
          case None =>
            EitherT.leftT[F, Value](
              InvalidResourceFormat(id.ref, "The provided payload could not be mapped to a Resolver"): Rejection)
        }
      }
  }
}
