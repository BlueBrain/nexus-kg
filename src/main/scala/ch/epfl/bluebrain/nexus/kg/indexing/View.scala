package ch.epfl.bluebrain.nexus.kg.indexing

import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

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
    * @return a generated name that uniquely identifies the view and its current revision
    */
  def name: String =
    s"${ref.id}_${uuid}_$rev"
}

object View {

  /**
    * ElasticSearch specific view.
    *
    * @param ref  a reference to the project that the view belongs to
    * @param id   the user facing view id
    * @param uuid the underlying uuid generated for this view
    * @param rev  the view revision
    */
  final case class ElasticView(ref: ProjectRef, id: AbsoluteIri, uuid: String, rev: Long) extends View

  /**
    * Sparql specific view.
    *
    * @param ref  a reference to the project that the view belongs to
    * @param id   the user facing view id
    * @param uuid the underlying uuid generated for this view
    * @param rev  the view revision
    */
  final case class SparqlView(ref: ProjectRef, id: AbsoluteIri, uuid: String, rev: Long) extends View

}
