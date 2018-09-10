package ch.epfl.bluebrain.nexus.kg.resolve

import cats.instances.all._
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.Contexts.resolverCtxUri
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder.resolverGraphEncoder
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.{DeprecatedId, RevisionedId}
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.cursor.GraphCursor
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.GraphResult
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.encoder._
import io.circe.Json

/**
  * Enumeration of Resolver types.
  */
sealed trait Resolver extends Product with Serializable {

  /**
    * @return a reference to the project that the resolver belongs to
    */
  def ref: ProjectRef

  /**
    * @return the resolver id
    */
  def id: AbsoluteIri

  /**
    * @return the resolver revision
    */
  def rev: Long

  /**
    * @return the deprecation state of the resolver
    */
  def deprecated: Boolean

  /**
    * @return the resolver priority
    */
  def priority: Int

  /**
    * Converts a resolver to a [[ResourceF.Value]]
    *
    * @param id            the id of the resource
    * @param flattenCtxObj the flatten context object (not wrapped in the @context key)
    */
  def resourceValue(id: ResId, flattenCtxObj: Json): ResourceF.Value = {
    val GraphResult(s, graph) = resolverGraphEncoder(this)
    val graphNoMeta           = graph.removeMetadata(id.value)
    val json = graphNoMeta
      .asJson(Json.obj("@context" -> flattenCtxObj), Some(s))
      .getOrElse(graphNoMeta.asJson)
      .removeKeys("@context")
      .addContext(resolverCtxUri)
    ResourceF.Value(json, flattenCtxObj, graphNoMeta)
  }
}

object Resolver {

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.resolve.Resolver]].
    *
    * @param res        a materialized resource
    * @param accountRef the account reference
    * @return Some(resolver) if the resource is compatible with a Resolver, None otherwise
    */
  final def apply(res: ResourceV, accountRef: AccountRef): Option[Resolver] = {
    val c = res.value.graph.cursor(res.id.value)

    def inProject: Option[Resolver] =
      for {
        priority <- c.downField(nxv.priority).focus.as[Int].toOption
      } yield InProjectResolver(res.id.parent, res.id.value, res.rev, res.deprecated, priority)

    def crossProject: Option[Resolver] =
      (for {
        projects <- c.downField(nxv.projects).values.asListOf[String].map(_.map(ProjectRef.apply))
        identities <- c.downField(nxv.identities).downArray.foldLeft[EncoderResult[List[Identity]]](Right(List.empty)) {
          case (err @ Left(_), _)   => err
          case (Right(list), inner) => identity(inner).map(_ :: list)
        }
        priority <- c.downField(nxv.priority).focus.as[Int]
        types = c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].getOrElse(List.empty[AbsoluteIri])
      } yield
        CrossProjectResolver(types.toSet,
                             projects.toSet,
                             identities,
                             res.id.parent,
                             res.id.value,
                             res.rev,
                             res.deprecated,
                             priority)).toOption

    def inAccount: Option[Resolver] =
      (for {
        identities <- c.downField(nxv.identities).downArray.foldLeft[EncoderResult[List[Identity]]](Right(List.empty)) {
          case (err @ Left(_), _)   => err
          case (Right(list), inner) => identity(inner).map(_ :: list)
        }
        priority <- c.downField(nxv.priority).focus.as[Int]
        types = c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].getOrElse(List.empty[AbsoluteIri])
      } yield
        InAccountResolver(types.toSet,
                          identities,
                          accountRef,
                          res.id.parent,
                          res.id.value,
                          res.rev,
                          res.deprecated,
                          priority)).toOption

    def identity(c: GraphCursor): EncoderResult[Identity] =
      c.downField(rdf.tpe).focus.as[AbsoluteIri].flatMap {
        case nxv.UserRef.value =>
          (c.downField(nxv.realm).focus.as[String], c.downField(nxv.sub).focus.as[String]).mapN(UserRef.apply)
        case nxv.GroupRef.value =>
          (c.downField(nxv.realm).focus.as[String], c.downField(nxv.group).focus.as[String]).mapN(GroupRef.apply)
        case nxv.AuthenticatedRef.value        => Right(AuthenticatedRef(c.downField(nxv.realm).focus.as[String].toOption))
        case iri if iri == nxv.Anonymous.value => Right(Anonymous)
        case t                                 => Left(IllegalConversion(s"The type '$t' cannot be converted into an Identity"))
      }

    if (Set(nxv.Resolver.value, nxv.CrossProject.value).subsetOf(res.types)) crossProject
    else if (Set(nxv.Resolver.value, nxv.InProject.value).subsetOf(res.types)) inProject
    else if (Set(nxv.Resolver.value, nxv.InAccount.value).subsetOf(res.types)) inAccount
    else None
  }

  /**
    * A resolver that looks only within its own project.
    */
  final case class InProjectResolver(
      ref: ProjectRef,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean,
      priority: Int
  ) extends Resolver

  /**
    * A resolver that looks within all projects belonging to its parent account.
    */
  final case class InAccountResolver(
      resourceTypes: Set[AbsoluteIri],
      identities: List[Identity],
      accountRef: AccountRef,
      ref: ProjectRef,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean,
      priority: Int
  ) extends Resolver

  /**
    * A resolver that can looks across several projects.
    */
  final case class CrossProjectResolver(
      resourceTypes: Set[AbsoluteIri],
      projects: Set[ProjectRef],
      identities: List[Identity],
      ref: ProjectRef,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean,
      priority: Int
  ) extends Resolver

  final implicit val resolverRevisionedId: RevisionedId[Resolver] = RevisionedId(r => (r.id, r.rev))
  final implicit val resolverDeprecatedId: DeprecatedId[Resolver] = DeprecatedId(r => (r.id, r.deprecated))

}
