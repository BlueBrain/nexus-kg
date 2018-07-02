package ch.epfl.bluebrain.nexus.kg.resolve

import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, ResourceV}
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.encoder._

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
}

object Resolver {

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.resolve.Resolver]].
    *
    * @param resource a materialized resource
    * @return Some(resolver) if the resource is compatible with a Resolver, None otherwise
    */
  final def apply(resource: ResourceV): Option[Resolver] =
    if (resource.types.contains(nxv.Resolver.value))
      resource.value.graph
        .cursor(resource.id.value)
        .downField(nxv.priority)
        .focus
        .as[Int]
        .map(InProjectResolver(resource.id.parent, resource.id.value, resource.rev, resource.deprecated, _))
        .toOption
    else None

  /**
    * A resolver that uses its project to resolve resources.
    *
    * @param ref        a reference to the project that the resolver belongs to
    * @param id         the resolver id
    * @param rev        the resolver revision
    * @param deprecated the deprecation state of the resolver
    * @param priority the resolver priority
    */
  final case class InProjectResolver(
      ref: ProjectRef,
      id: AbsoluteIri,
      rev: Long,
      deprecated: Boolean,
      priority: Int
  ) extends Resolver
}
