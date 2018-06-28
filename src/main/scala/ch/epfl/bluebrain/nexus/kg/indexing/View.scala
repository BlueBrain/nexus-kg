package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, ResourceV}
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node._

import scala.util.Try

/**
  * Enumeration of view types.
  */
sealed trait View extends Product with Serializable {

  /**
    * @return a reference to the project that the view belongs to
    */
  def ref: ProjectRef

  /**
    * @return the user facing view id
    */
  def id: AbsoluteIri

  /**
    * @return the underlying uuid generated for this view
    */
  def uuid: String

  /**
    * @return the view revision
    */
  def rev: Long

  /**
    * @return the deprecation state of the view
    */
  def deprecated: Boolean

  /**
    * @return a generated name that uniquely identifies the view and its current revision
    */
  def name: String =
    s"${ref.id}_${uuid}_$rev"
}

object View {

  /**
    * Attempts to transform the resource into a [[ch.epfl.bluebrain.nexus.kg.indexing.View]].
    *
    * @param resource a materialized resource
    * @return Some(view) if the resource is compatible with a View, None otherwise
    */
  final def apply(resource: ResourceV): Option[View] =
    if (resource.types.contains(nxv.View.value))
      resource.value.graph.cursor(resource.id.value).downField(nxv.uuid).values match {
        case Some(values) =>
          val uuids = values.flatMap { n =>
            n.asLiteral
              .filter(_.isString)
              .flatMap(l => Try(UUID.fromString(l.lexicalForm)).map(_.toString.toLowerCase).toOption)
              .toIterable
          }
          uuids.headOption.flatMap { uuid =>
            if (resource.types.contains(nxv.ElasticView.value))
              Some(
                ElasticView(
                  resource.id.parent,
                  resource.id.value,
                  uuid,
                  resource.rev,
                  resource.deprecated
                ))
            else if (resource.types.contains(nxv.SparqlView.value))
              Some(
                SparqlView(
                  resource.id.parent,
                  resource.id.value,
                  uuid,
                  resource.rev,
                  resource.deprecated
                ))
            else None
          }
        case None => None
      } else None

  /**
    * ElasticSearch specific view.
    *
    * @param ref        a reference to the project that the view belongs to
    * @param id         the user facing view id
    * @param uuid       the underlying uuid generated for this view
    * @param rev        the view revision
    * @param deprecated the deprecation state of the view
    */
  final case class ElasticView(
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: String,
      rev: Long,
      deprecated: Boolean
  ) extends View

  /**
    * Sparql specific view.
    *
    * @param ref        a reference to the project that the view belongs to
    * @param id         the user facing view id
    * @param uuid       the underlying uuid generated for this view
    * @param rev        the view revision
    * @param deprecated the deprecation state of the view
    */
  final case class SparqlView(
      ref: ProjectRef,
      id: AbsoluteIri,
      uuid: String,
      rev: Long,
      deprecated: Boolean
  ) extends View
}
