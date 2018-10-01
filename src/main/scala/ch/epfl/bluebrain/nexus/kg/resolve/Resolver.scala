package ch.epfl.bluebrain.nexus.kg.resolve

import java.util.UUID

import cats.data.EitherT
import cats.{Monad, Show}
import cats.instances.all._
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts.resolverCtxUri
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder.resolverGraphEncoder
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{LabelsNotFound, ProjectsNotFound}
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
import scala.util.Try

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

  /**
    * Attempts to convert the current resolver to a labeled resolver when required. This conversion is only targetting ''CrossProjectResolver[ProjectRef]'',
    * returning all the other resolvers unchanged.
    * For the case of ''CrossProjectResolver[ProjectRef]'', the conversion is successful when the the mapping ''projectRef -> projectLabel'' exists on the ''cache''
    */
  def labeled[F[_]](implicit cache: DistributedCache[F], F: Monad[F]): EitherT[F, Rejection, Resolver] =
    this match {
      case CrossProjectRefs(r) =>
        EitherT(r.projects.map(ref => ref.toLabel(cache).map(ref -> _)).toList.sequence.map { list =>
          val failed = list.collect { case (v, None) => v }
          if (failed.nonEmpty)
            Left(LabelsNotFound(failed))
          else {
            val succeed = list.collect { case (_, Some(v)) => v }.toSet
            Right(CrossProjectResolver(r.resourceTypes, succeed, r.identities, ref, id, rev, deprecated, priority))
          }
        })
      case o => EitherT.rightT(o)
    }

  /**
    * Attempts to convert the current resolver to a referenced resolver when required. This conversion is only targetting ''CrossProjectResolver[ProjectLabel]'',
    * returning all the other resolvers unchanged.
    * For the case of ''CrossProjectResolver[ProjectLabel]'', the conversion is successful when the the mapping ''projectLabel -> projectRef'' exists on the ''cache''
    */
  def referenced[F[_]](implicit cache: DistributedCache[F], F: Monad[F]): EitherT[F, Rejection, Resolver] =
    this match {
      case CrossProjectLabels(r) =>
        EitherT(r.projects.map(l => cache.projectRef(l).map(l -> _)).toList.sequence.map { list =>
          val failed = list.collect { case (v, None) => v }
          if (failed.nonEmpty)
            Left(ProjectsNotFound(failed))
          else {
            val succeed = list.collect { case (_, Some(v)) => v }.toSet
            Right(CrossProjectResolver(r.resourceTypes, succeed, r.identities, ref, id, rev, deprecated, priority))
          }
        })
      case o => EitherT.rightT(o)
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

    def projectToLabel(value: String): Option[ProjectLabel] =
      value.trim.split("/") match {
        case Array(account, project) => Some(ProjectLabel(account, project))
        case _                       => None
      }
    def projectToRefs(value: String): Option[ProjectRef] =
      Try(UUID.fromString(value)).map(_ => ProjectRef(value)).toOption

    def crossProject: Option[CrossProjectResolver[_]] =
      // format: off
      (for {
        strings     <- c.downField(nxv.projects).values.asListOf[String].map(_.toSet)
        labels       = strings.flatMap(projectToLabel)
        refs         = if(labels.isEmpty) strings.flatMap(projectToRefs) else Set.empty[ProjectRef]
        identities  <- identities(c.downField(nxv.identities).downArray)
        priority    <- c.downField(nxv.priority).focus.as[Int]
        types        = c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].getOrElse(List.empty[AbsoluteIri]).toSet
      } yield {
        if(labels.nonEmpty)
          CrossProjectResolver(types, labels, identities, res.id.parent, res.id.value, res.rev, res.deprecated, priority)
        else if(refs.nonEmpty)
          CrossProjectResolver(types, refs, identities, res.id.parent, res.id.value, res.rev, res.deprecated, priority)
        else
          CrossProjectResolver(types, strings, identities, res.id.parent, res.id.value, res.rev, res.deprecated, priority)

      }).toOption
    // format: on

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

    def identities(list: Iterable[GraphCursor]) =
      list.foldLeft[EncoderResult[List[Identity]]](Right(List.empty)) {
        case (err @ Left(_), _)   => err
        case (Right(list), inner) => identity(inner).map(_ :: list)
      }

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
  final case class CrossProjectResolver[P: Show](
      resourceTypes: Set[AbsoluteIri],
      projects: Set[P],
      identities: List[Identity],
      ref: ProjectRef,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean,
      priority: Int
  ) extends Resolver {
    val projectsString: Set[String] = projects.map(_.show)
  }

  type CrossProjectLabels = CrossProjectResolver[ProjectLabel]
  object CrossProjectLabels {
    final def unapply(arg: CrossProjectResolver[_]): Option[CrossProjectLabels] =
      arg.projects.toSeq match {
        case (_: ProjectLabel) +: _ => Some(arg.asInstanceOf[CrossProjectLabels])
        case _                      => None
      }
  }

  type CrossProjectRefs = CrossProjectResolver[ProjectRef]
  object CrossProjectRefs {
    final def unapply(arg: CrossProjectResolver[_]): Option[CrossProjectRefs] =
      arg.projects.toSeq match {
        case (_: ProjectRef) +: _ => Some(arg.asInstanceOf[CrossProjectRefs])
        case _                    => None
      }
  }

  final implicit val resolverRevisionedId: RevisionedId[Resolver] = RevisionedId(r => (r.id, r.rev))
  final implicit val resolverDeprecatedId: DeprecatedId[Resolver] = DeprecatedId(r => (r.id, r.deprecated))

}
