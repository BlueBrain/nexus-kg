package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.{Applicative, Monad, MonadError}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.iam.client.types.{FullAccessControlList, Identity}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InAccountResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resources.AdditionalValidation._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidIdentity, InvalidPayload, ProjectNotFound}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

trait AdditionalValidation[F[_]] {

  /**
    * Performs additional validation
    *
    * @param id     the unique identifier of the resource
    * @param schema the schema that this resource conforms to
    * @param types  the collection of known types of this resource
    * @param value  the resource value
    * @return a Left(rejection) when the validation does not pass or Right(value) when it does on the effect type ''F''
    */
  def apply(id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value): EitherT[F, Rejection, Value]
}

object AdditionalValidation {

  type Value = ResourceF.Value

  /**
    * @tparam F the monadic effect type
    * @return a new validation that always returns Right(value) on the provided effect type
    */
  final def pass[F[_]: Applicative]: AdditionalValidation[F] =
    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value) => EitherT.rightT(value)

  /**
    * Additional validation used for checking the correctness of the ElasticSearch mappings
    *
    * @tparam F the monadic effect type
    * @return a new validation that passes whenever the provided mappings are compliant with the ElasticSearch mappings or
    *         when the view is not an ElasticView
    */
  final def view[F[_]](implicit F: MonadError[F, Throwable],
                       elastic: ElasticClient[F],
                       config: ElasticConfig): AdditionalValidation[F] =
    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value) => {
      val resource = ResourceF.simpleV(id, value, types = types, schema = schema)
      EitherT.fromEither(View(resource)).flatMap {
        case es: ElasticView => EitherT(es.createIndex[F]).map(_ => value)
        case _               => EitherT.rightT(value)
      }
    }

  /**
    * Additional validation used for checking ACLs on [[Resolver]] creation
    *
    * @param acls       the [[FullAccessControlList]]
    * @param accountRef the account reference
    * @tparam F the monadic effect type
    * @return a new validation that passes whenever the provided ''acls'' match the ones on the resolver's identities
    */
  final def resolver[F[_]: Monad](acls: Option[FullAccessControlList],
                                  accountRef: AccountRef,
                                  labelResolution: ProjectLabel => F[Option[ProjectRef]]): AdditionalValidation[F] = {
    def aclContains(identities: List[Identity]): Boolean = {
      val list = acls.map(_.acl.map(_.identity)).getOrElse(List.empty)
      identities.forall(list.contains)
    }

    def projectToLabel(value: String): Option[ProjectLabel] =
      value.trim.split("/") match {
        case Array(account, project) => Some(ProjectLabel(account, project))
        case _                       => None
      }

    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value) =>
      {
        def invalidRef(ref: String): Rejection =
          InvalidPayload(id.ref, s"'projects' values must be formatted as {account}/{project} and not as '$ref'")

        def projectNotFound(label: ProjectLabel): Rejection =
          ProjectNotFound(label)

        val resource = ResourceF.simpleV(id, value, types = types, schema = schema)
        Resolver(resource, accountRef) match {
          case Some(resolver: CrossProjectResolver) if aclContains(resolver.identities) =>
            resolver.projects
              .foldLeft(EitherT.rightT[F, Rejection](Set.empty[ProjectRef])) { (acc, c) =>
                for {
                  set    <- acc
                  label  <- EitherT.fromOption[F](projectToLabel(c.id), invalidRef(c.id))
                  ref    <- EitherT.fromOptionF(labelResolution(label), projectNotFound(label))
                  newSet <- EitherT.rightT[F, Rejection](set + ref)
                } yield newSet
              }
              .map(projects => resolver.copy(projects = projects).resourceValue(id, value.ctx))
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
