package ch.epfl.bluebrain.nexus.kg.resources

import cats.Applicative
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.iam.client.types.{FullAccessControlList, Identity}
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InAccountResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidIdentity, InvalidPayload}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

trait AdditionalValidation[F[_]] {

  /**
    * Performs additional validation
    *
    * @param id     the unique identifier of the resource
    * @param schema the schema that this resource conforms to
    * @param types  the collection of known types of this resource
    * @param value  the resource value
    * @return a Left(rejection) when the validation does not pass or Right(()) when it does on the effect type ''F''
    */
  def apply(id: ResId, schema: Ref, types: Set[AbsoluteIri], value: ResourceF.Value): EitherT[F, Rejection, Unit]
}

object AdditionalValidation {

  /**
    * @tparam F the monadic effect type
    * @return a new validation that always returns Right(()) on the provided effect type
    */
  final def pass[F[_]: Applicative]: AdditionalValidation[F] =
    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: ResourceF.Value) => EitherT.rightT[F, Rejection](())

  /**
    * Additional validation used for checking ACLs on [[Resolver]] creation
    *
    * @param acls the [[FullAccessControlList]]
    * @tparam F the monadic effect type
    * @return a new validation that passes whenever the provided ''acls'' match the ones on the resolver's identities
    */
  final def resolver[F[_]: Applicative](acls: Option[FullAccessControlList]): AdditionalValidation[F] = {
    def aclContains(identities: List[Identity]): Boolean = {
      val list = acls.map(_.acl.map(_.identity)).getOrElse(List.empty)
      identities.forall(list.contains)
    }

    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: ResourceF.Value) =>
      {
        val resource = ResourceF.simpleV(id, value, types = types, schema = schema)
        Resolver(resource) match {
          case Some(resolver: CrossProjectResolver) if aclContains(resolver.identities) => EitherT.rightT(())
          case Some(resolver: InAccountResolver) if aclContains(resolver.identities)    => EitherT.rightT(())
          case Some(_: InProjectResolver)                                               => EitherT.rightT(())
          case Some(_)                                                                  => EitherT.leftT(InvalidIdentity("Your account does not contain all the identities provided"))
          case None =>
            EitherT.leftT(InvalidPayload(id.ref, "The provided payload could not be mapped to a Resolver"): Rejection)
        }
      }
  }
}
