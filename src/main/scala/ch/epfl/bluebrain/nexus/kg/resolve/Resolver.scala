package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.data.EitherT
import cats.implicits._
import ch.epfl.bluebrain.nexus.iam.client.types.{Identity, Permission}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg._
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidResourceFormat, LabelsNotFound, ProjectsNotFound}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.cursor.GraphCursor
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import shapeless.TypeCase

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
    * Attempts to convert the current resolver to a labeled resolver when required. This conversion is only targetting ''CrossProjectResolver[ProjectRef]'',
    * returning all the other resolvers unchanged.
    * For the case of ''CrossProjectResolver[ProjectRef]'', the conversion is successful when the the mapping ''projectRef -> projectLabel'' exists on the ''cache''
    */
  def labeled[F[_]](implicit projectCache: ProjectCache[F], F: Monad[F]): EitherT[F, Rejection, Resolver] =
    this match {
      case r @ CrossProjectResolver(_, `Set[ProjectRef]`(projectRefs), _, _, _, _, _, _) =>
        EitherT(projectCache.getProjectLabels(projectRefs).map(resultOrFailures).map {
          case Right(result)  => Right(r.copy(projects = result.map { case (_, label) => label }.toSet))
          case Left(projects) => Left(LabelsNotFound(projects))
        })
      case o =>
        EitherT.rightT(o)
    }

  /**
    * Attempts to convert the current resolver to a referenced resolver when required. This conversion is only targetting ''CrossProjectResolver[ProjectLabel]'',
    * returning all the other resolvers unchanged.
    * For the case of ''CrossProjectResolver[ProjectLabel]'', the conversion is successful when the the mapping ''projectLabel -> projectRef'' exists on the ''cache''
    */
  def referenced[F[_]](implicit projectCache: ProjectCache[F], F: Monad[F]): EitherT[F, Rejection, Resolver] =
    this match {
      case r @ CrossProjectResolver(_, `Set[ProjectLabel]`(projectLabels), _, _, _, _, _, _) =>
        EitherT(projectCache.getProjectRefs(projectLabels).map(resultOrFailures).map {
          case Right(result)  => Right(r.copy(projects = result.map { case (_, ref) => ref }.toSet))
          case Left(projects) => Left(ProjectsNotFound(projects))
        })
      case o =>
        EitherT.rightT(o)
    }
}

object Resolver {

  val write: Permission = Permission.unsafe("resolvers/write")

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.resolve.Resolver]].
    *
    * @param res             a materialized resource
    * @return Some(resolver) if the resource is compatible with a Resolver, None otherwise
    */
  final def apply(res: ResourceV): Either[Rejection, Resolver] = {
    val c  = res.value.graph.cursor()
    val id = res.id

    def inProject: Either[Rejection, Resolver] =
      for {
        priority <- c.downField(nxv.priority).focus.as[Int].toRejectionOnLeft(res.id.ref)
      } yield InProjectResolver(id.parent, id.value, res.rev, res.deprecated, priority)

    def crossProject: Either[Rejection, CrossProjectResolver[_]] = {
      // format: off
      val result = for {
        ids   <- identities(c.downField(nxv.identities).downArray)
        prio  <- c.downField(nxv.priority).focus.as[Int].toRejectionOnLeft(res.id.ref)
        types <- c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].orElse(List.empty).map(_.toSet).toRejectionOnLeft(res.id.ref)
      } yield CrossProjectResolver(types, Set.empty[String], ids, id.parent, id.value, res.rev, res.deprecated, prio)
      // format: on
      result.flatMap { r =>
        val nodes = c.downField(nxv.projects).values
        nodes.asListOf[ProjectLabel].toRejectionOnLeft(res.id.ref) match {
          case Right(projectLabels) => Right(r.copy(projects = projectLabels.toSet))
          case Left(_) =>
            nodes.asListOf[ProjectRef].toRejectionOnLeft(res.id.ref).map(refs => r.copy(projects = refs.toSet))
        }

      }
    }

    def identities(iter: Iterable[GraphCursor]): Either[Rejection, List[Identity]] =
      iter.toList.foldM(List.empty[Identity]) { (acc, innerCursor) =>
        identity(innerCursor).map(_ :: acc)
      }

    def identity(c: GraphCursor): Either[Rejection, Identity] = {
      lazy val anonymous =
        c.downField(rdf.tpe).focus.as[AbsoluteIri].toOption.collectFirst { case nxv.Anonymous.value => Anonymous }
      lazy val realm         = c.downField(nxv.realm).focus.as[String]
      lazy val user          = (c.downField(nxv.subject).focus.as[String], realm).mapN(User.apply).toOption
      lazy val group         = (c.downField(nxv.group).focus.as[String], realm).mapN(Group.apply).toOption
      lazy val authenticated = realm.map(Authenticated.apply).toOption
      (anonymous orElse user orElse group orElse authenticated).toRight(
        InvalidResourceFormat(
          res.id.ref,
          "The provided payload could not be mapped to a resolver because the identity format is wrong"
        )
      )
    }

    if (Set(nxv.Resolver.value, nxv.CrossProject.value).subsetOf(res.types)) crossProject
    else if (Set(nxv.Resolver.value, nxv.InProject.value).subsetOf(res.types)) inProject
    else Left(InvalidResourceFormat(res.id.ref, "The provided @type do not match any of the resolver types"))
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

  object InProjectResolver {

    /**
      * Default [[InProjectResolver]] that gets created for every project.
      *
      * @param ref the project unique identifier
      */
    def default(ref: ProjectRef): InProjectResolver =
      InProjectResolver(ref, nxv.defaultResolver.value, 1L, deprecated = false, 1)
  }

  /**
    * A resolver that can look across several projects.
    */
  final case class CrossProjectResolver[P](
      resourceTypes: Set[AbsoluteIri],
      projects: Set[P],
      identities: List[Identity],
      ref: ProjectRef,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean,
      priority: Int
  ) extends Resolver

  private[resolve] val `Set[ProjectRef]`   = TypeCase[Set[ProjectRef]]
  private[resolve] val `Set[ProjectLabel]` = TypeCase[Set[ProjectLabel]]
}
