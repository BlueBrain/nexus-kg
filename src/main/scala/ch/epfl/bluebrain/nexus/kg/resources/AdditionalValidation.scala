package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.{Applicative, Monad}
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.iam.client.types.{FullAccessControlList, Identity}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InAccountResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidIdentity, InvalidPayload, ProjectNotFound}
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.GraphResult
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import io.circe.Json

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
  def apply(id: ResId,
            schema: Ref,
            types: Set[AbsoluteIri],
            value: ResourceF.Value): EitherT[F, Rejection, ResourceF.Value]
}

object AdditionalValidation {

  /**
    * @tparam F the monadic effect type
    * @return a new validation that always returns Right(value) on the provided effect type
    */
  final def pass[F[_]: Applicative]: AdditionalValidation[F] =
    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: ResourceF.Value) => EitherT.rightT(value)

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
    type Value = ResourceF.Value

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
        def toResourceV(resolver: Resolver) = {
          val GraphResult(s, graph) = resolverGraphEncoder(resolver)
          val finalGraph = graph -- Graph(
            Set[Triple]((id.value, nxv.rev, resolver.rev), (id.value, nxv.deprecated, resolver.deprecated)))
          val json =
            finalGraph
              .asJson(Json.obj("@context" -> value.ctx), Some(s))
              .getOrElse(graph.asJson)
              .removeKeys("@context")
              .addContext(resolverCtxUri)
          ResourceF.Value(json, value.ctx, graph)
        }

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
              .map(projects => toResourceV(resolver.copy(projects = projects)))
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
