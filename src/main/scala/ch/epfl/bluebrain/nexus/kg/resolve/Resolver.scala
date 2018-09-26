package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.data.EitherT
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts.resolverCtxUri
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder.resolverGraphEncoder
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
    *
    * Enumeration of Resolver types that are stored in the primary store.
    */
  sealed trait StoredResolver extends Resolver

  /**
    *
    * Enumeration of Resolver types that are exposed though the API.
    */
  sealed trait ExposedResolver extends Resolver

  private def inProject(res: ResourceV, c: GraphCursor): Option[InProjectResolver] =
    for {
      priority <- c.downField(nxv.priority).focus.as[Int].toOption
    } yield InProjectResolver(res.id.parent, res.id.value, res.rev, res.deprecated, priority)

  private def inAccount(res: ResourceV, accountRef: AccountRef, c: GraphCursor): Option[InAccountResolver] =
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

  private def identity(c: GraphCursor): EncoderResult[Identity] =
    c.downField(rdf.tpe).focus.as[AbsoluteIri].flatMap {
      case nxv.UserRef.value =>
        (c.downField(nxv.realm).focus.as[String], c.downField(nxv.sub).focus.as[String]).mapN(UserRef.apply)
      case nxv.GroupRef.value =>
        (c.downField(nxv.realm).focus.as[String], c.downField(nxv.group).focus.as[String]).mapN(GroupRef.apply)
      case nxv.AuthenticatedRef.value        => Right(AuthenticatedRef(c.downField(nxv.realm).focus.as[String].toOption))
      case iri if iri == nxv.Anonymous.value => Right(Anonymous)
      case t                                 => Left(IllegalConversion(s"The type '$t' cannot be converted into an Identity"))
    }

  /**
    * Attempts to transform the resource into a [[ExposedResolver]].
    *
    * @param res        a materialized resource
    * @param accountRef the account reference
    * @return Some(exposedResolver) if the resource is compatible with a ExposedResolver, None otherwise
    */
  final def exposed(res: ResourceV, accountRef: AccountRef): Option[ExposedResolver] = {
    val c = res.value.graph.cursor(res.id.value)

    def projectToLabel(value: String): Option[ProjectLabel] =
      value.trim.split("/") match {
        case Array(account, project) => Some(ProjectLabel(account, project))
        case _                       => None
      }

    def crossProjectLabel: Option[ExposedResolver] =
      (for {
        projects <- c.downField(nxv.projects).values.asListOf[String].map(_.flatMap(projectToLabel))
        identities <- c.downField(nxv.identities).downArray.foldLeft[EncoderResult[List[Identity]]](Right(List.empty)) {
          case (err @ Left(_), _)   => err
          case (Right(list), inner) => identity(inner).map(_ :: list)
        }
        priority <- c.downField(nxv.priority).focus.as[Int]
        types = c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri].getOrElse(List.empty[AbsoluteIri])
      } yield
        CrossProjectLabelResolver(types.toSet,
                                  projects.toSet,
                                  identities,
                                  res.id.parent,
                                  res.id.value,
                                  res.rev,
                                  res.deprecated,
                                  priority)).toOption

    if (Set(nxv.Resolver.value, nxv.CrossProject.value).subsetOf(res.types)) crossProjectLabel
    else if (Set(nxv.Resolver.value, nxv.InProject.value).subsetOf(res.types)) inProject(res, c)
    else if (Set(nxv.Resolver.value, nxv.InAccount.value).subsetOf(res.types)) inAccount(res, accountRef, c)
    else None
  }

  /**
    * Attempts to transform the resource into a [[StoredResolver]].
    *
    * @param res        a materialized resource
    * @param accountRef the account reference
    * @return Some(storedResolver) if the resource is compatible with a StoredResolver, None otherwise
    */
  final def stored(res: ResourceV, accountRef: AccountRef): Option[StoredResolver] = {
    val c = res.value.graph.cursor(res.id.value)

    def crossProject: Option[StoredResolver] =
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

    if (Set(nxv.Resolver.value, nxv.CrossProject.value).subsetOf(res.types)) crossProject
    else if (Set(nxv.Resolver.value, nxv.InProject.value).subsetOf(res.types)) inProject(res, c)
    else if (Set(nxv.Resolver.value, nxv.InAccount.value).subsetOf(res.types)) inAccount(res, accountRef, c)
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
  ) extends StoredResolver
      with ExposedResolver

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
  ) extends StoredResolver
      with ExposedResolver

  /**
    * A resolver that can looks across several projects. The different between this and the [[CrossProjectLabelResolver]]
    * is on the way to represent the ''projects''
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
  ) extends StoredResolver {

    /**
      * Attempts to convert the current resolver to a [[CrossProjectLabelResolver]] and signal the error when the conversion fails.
      * For the conversion to succeed, the the mapping ''projectRef -> projectLabel'' has to exist on the ''cache''.
      */
    def toExposed[F[_]](implicit cache: DistributedCache[F],
                        F: Monad[F]): EitherT[F, Rejection, CrossProjectLabelResolver] =
      EitherT(projects.map(ref => ref.toLabel(cache).map(ref -> _)).toList.sequence.map { r =>
        val failed = r.collect { case (r, None) => r }
        if (failed.nonEmpty)
          Left(LabelsNotFound(failed))
        else {
          val succeed = r.collect { case (_, Some(l)) => l }
          Right(CrossProjectLabelResolver(resourceTypes, succeed.toSet, identities, ref, id, rev, deprecated, priority))
        }
      })
  }

  /**
    * A resolver that can looks across several projects. The different between this and the [[CrossProjectResolver]]
    * is on the way to represent the ''projects''
    */
  final case class CrossProjectLabelResolver(
      resourceTypes: Set[AbsoluteIri],
      projects: Set[ProjectLabel],
      identities: List[Identity],
      ref: ProjectRef,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean,
      priority: Int
  ) extends ExposedResolver {

    /**
      * Attempts to convert the current resolver to a [[CrossProjectResolver]] and signal the error when the conversion fails.
      * For the conversion to succeed, the the mapping ''projectLabel -> projectRef'' has to exist on the ''cache''.
      */
    def toStored[F[_]](implicit cache: DistributedCache[F], F: Monad[F]): EitherT[F, Rejection, CrossProjectResolver] =
      EitherT(projects.map(l => cache.projectRef(l).map(l -> _)).toList.sequence.map { r =>
        val failed = r.collect { case (l, None) => l }
        if (failed.nonEmpty)
          Left(ProjectsNotFound(failed))
        else {
          val succeed = r.collect { case (_, Some(r)) => r }
          Right(CrossProjectResolver(resourceTypes, succeed.toSet, identities, ref, id, rev, deprecated, priority))
        }
      })
  }

  final implicit val resolverRevisionedId: RevisionedId[StoredResolver] = RevisionedId(r => (r.id, r.rev))
  final implicit val resolverDeprecatedId: DeprecatedId[StoredResolver] = DeprecatedId(r => (r.id, r.deprecated))

}
