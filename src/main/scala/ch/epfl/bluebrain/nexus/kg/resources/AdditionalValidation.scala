package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.data.EitherT
import cats.syntax.all._
import cats.{Applicative, Monad, MonadError}
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticFailure}
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.iam.client.types.{FullAccessControlList, Identity}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InAccountResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.AdditionalValidation._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidIdentity, InvalidPayload, ProjectNotFound, Unexpected}
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Graph.{Triple, _}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.GraphResult
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.encoder._
import io.circe.Json
import io.circe.parser.parse

import scala.util.Try

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
  final def view[F[_]](implicit F: MonadError[F, Throwable], elastic: ElasticClient[F]): AdditionalValidation[F] =
    (id: ResId, schema: Ref, types: Set[AbsoluteIri], value: Value) => {
      if (types.contains(nxv.ElasticView.value)) {
        val verifyMapping: F[Either[Rejection, Value]] = {
          val index = UUID.randomUUID().toString
          value.graph.cursor(id.value).downField(nxv.mapping).focus.as[String].flatMap(parse) match {
            case Left(_) =>
              F.pure(Left(InvalidPayload(id.ref, "ElasticSearch mapping field not found or not convertible to Json")))
            case Right(mapping) =>
              elastic
                .createIndex(index, mapping)
                .flatMap[Either[Rejection, Value]] {
                  case true  => elastic.deleteIndex(index).map(_ => Right(value))
                  case false => F.pure(Left(Unexpected("View mapping validation could not be performed")))
                }
                .recoverWith {
                  case err: ElasticFailure => F.pure(Left(InvalidPayload(id.ref, err.body)))
                  case err =>
                    val msg = Try(err.getMessage).getOrElse("")
                    F.pure(Left(Unexpected(s"View mapping validation could not be performed. Cause '$msg'")))
                }
          }
        }
        EitherT(verifyMapping)
      } else
        EitherT.rightT[F, Rejection](value)

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
