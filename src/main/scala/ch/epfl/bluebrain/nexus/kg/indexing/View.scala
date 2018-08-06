package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.async.RevisionedId
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, ResourceV}
import ch.epfl.bluebrain.nexus.rdf.Graph._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.encoder._

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
    * @param res a materialized resource
    * @return Some(view) if the resource is compatible with a View, None otherwise
    */
  final def apply(res: ResourceV): Option[View] =
    if (res.types.contains(nxv.View.value))
      res.value.graph.cursor(res.id.value).downField(nxv.uuid).focus.as[UUID].toOption.flatMap { uuid =>
        if (res.types.contains(nxv.ElasticView.value))
          Some(ElasticView(res.id.parent, res.id.value, uuid.toString.toLowerCase(), res.rev, res.deprecated))
        else if (res.types.contains(nxv.SparqlView.value))
          Some(SparqlView(res.id.parent, res.id.value, uuid.toString.toLowerCase(), res.rev, res.deprecated))
        else None
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

  final implicit val viewRevisionedId: RevisionedId[View] = RevisionedId(view => (view.id, view.rev))
}
