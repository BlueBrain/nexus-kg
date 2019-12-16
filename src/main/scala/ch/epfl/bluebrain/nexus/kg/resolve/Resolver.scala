package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.data.EitherT
import cats.implicits._
import ch.epfl.bluebrain.nexus.iam.client.types.{Identity, Permission}
import ch.epfl.bluebrain.nexus.kg._
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import shapeless.{TypeCase, Typeable}

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
    * Converts the ProjectRefs into ProjectLabels when found on the cache
    */
  def labeled[F[_]: Monad](implicit projectCache: ProjectCache[F]): EitherT[F, Rejection, Resolver] =
    this match {
      case r: CrossProjectResolver => r.projects.traverse(_.toLabel[F]).map(labels => r.copy(projects = labels))
      case r                       => EitherT.rightT(r)
    }

  /**
    * Converts the ProjectLabels into ProjectRefs when found on the cache
    */
  def referenced[F[_]: Monad](implicit projectCache: ProjectCache[F]): EitherT[F, Rejection, Resolver] =
    this match {
      case r: CrossProjectResolver => r.projects.traverse(_.toRef[F]).map(refs => r.copy(projects = refs))
      case r                       => EitherT.rightT(r)
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
        priority <- c.downField(nxv.priority).focus.as[Int].onError(res.id.ref, nxv.priority.prefix)
      } yield InProjectResolver(id.parent, id.value, res.rev, res.deprecated, priority)

    def crossProject: Either[Rejection, CrossProjectResolver] =
      // format: off
      for {
        ids       <- identities(res.id, c.downField(nxv.identities).downSet)
        prio      <- c.downField(nxv.priority).focus.as[Int].onError(res.id.ref, nxv.priority.prefix)
        projects  <- c.downField(nxv.projects).values.asListOf[ProjectIdentifier].onError(res.id.ref, nxv.projects.prefix)
        types     <- c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].withDefault(List.empty).map(_.toSet).onError(res.id.ref, nxv.resourceTypes.prefix)
      } yield CrossProjectResolver(types, projects.toList, ids, id.parent, id.value, res.rev, res.deprecated, prio)
      // format: on

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
  final case class CrossProjectResolver(
      resourceTypes: Set[AbsoluteIri],
      projects: List[ProjectIdentifier],
      identities: List[Identity],
      ref: ProjectRef,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean,
      priority: Int
  ) extends Resolver {

    def projectsBy[T <: ProjectIdentifier: Typeable]: List[T] = {
      val tpe = TypeCase[T]
      projects.collect { case tpe(project) => project }
    }
  }
}
