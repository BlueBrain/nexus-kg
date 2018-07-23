package ch.epfl.bluebrain.nexus.kg.resolve

import cats.instances.all._
import cats.syntax.all._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectRef, ResourceV}
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.cursor.GraphCursor
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.encoder._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}

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
    * The resolver [[Graph]] representation
    */
  def asGraph: Graph

  private[resolve] val s = IriNode(id)

  private[resolve] def mainGraph(tpe: AbsoluteIri): Graph =
    Graph(
      (s, rdf.tpe, nxv.Resolver),
      (s, rdf.tpe, tpe),
      (s, nxv.priority, priority),
      (s, nxv.deprecated, deprecated),
      (s, nxv.rev, rev)
    )
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
        types    <- c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri]
        projects <- c.downField(nxv.projects).values.asListOf[String].map(_.map(ProjectRef.apply))
        identities <- c.downField(nxv.identities).downArray.foldLeft[EncoderResult[List[Identity]]](Right(List.empty)) {
          case (err @ Left(_), _)   => err
          case (Right(list), inner) => identity(inner).map(_ :: list)
        }
        priority <- c.downField(nxv.priority).focus.as[Int]
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
        types <- c.downField(nxv.resourceTypes).values.asListOf[AbsoluteIri]
        identities <- c.downField(nxv.identities).downArray.foldLeft[EncoderResult[List[Identity]]](Right(List.empty)) {
          case (err @ Left(_), _)   => err
          case (Right(list), inner) => identity(inner).map(_ :: list)
        }
        priority <- c.downField(nxv.priority).focus.as[Int]
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

    if (res.types.contains(nxv.Resolver.value) && res.types.contains(nxv.CrossProject.value)) crossProject
    else if (res.types.contains(nxv.Resolver.value) && res.types.contains(nxv.InProject.value)) inProject
    else if (res.types.contains(nxv.Resolver.value) && res.types.contains(nxv.InAccount.value)) inAccount
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
  ) extends Resolver {
    val asGraph: Graph = mainGraph(nxv.InProject)
  }

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
  ) extends Resolver {
    val asGraph: Graph = mainGraph(nxv.InAccount) ++ graphFor(identities)(s) ++ graphFor(resourceTypes)(s)
  }

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
  ) extends Resolver {
    private val graphForProjects = Graph(projects.map(r => (s: IriOrBNode, nxv.projects, r.id: Node)))

    val asGraph: Graph =
      mainGraph(nxv.InAccount) ++ graphFor(identities)(s) ++ graphFor(resourceTypes)(s) ++ graphForProjects
  }

  private def graphFor(identity: Identity): (BNode, Graph) = {
    val ss = blank
    identity match {
      case UserRef(realm, sub)           => ss -> Graph((ss, rdf.tpe, nxv.UserRef), (ss, nxv.realm, realm), (ss, nxv.sub, sub))
      case GroupRef(realm, g)            => ss -> Graph((ss, rdf.tpe, nxv.GroupRef), (ss, nxv.realm, realm), (ss, nxv.group, g))
      case AuthenticatedRef(Some(realm)) => ss -> Graph((ss, rdf.tpe, nxv.AuthenticatedRef), (ss, nxv.realm, realm))
      case AuthenticatedRef(_)           => ss -> Graph((ss, rdf.tpe, nxv.AuthenticatedRef))
      case _                             => ss -> Graph((ss, rdf.tpe, nxv.Anonymous))
    }
  }

  private def graphFor(identities: List[Identity])(s: IriNode): Graph =
    identities.foldLeft(Graph()) { (finalGraph, identity) =>
      val (bNode, graph) = graphFor(identity)
      finalGraph + ((s, nxv.identities, bNode)) ++ graph
    }

  private def graphFor(resourceTypes: Set[AbsoluteIri])(s: IriNode): Graph =
    Graph(resourceTypes.map(r => (s: IriOrBNode, nxv.resourceTypes, IriNode(r): Node)))

}
