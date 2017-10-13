package ch.epfl.bluebrain.nexus.kg.service.hateoas

import akka.http.scaladsl.model.Uri

/**
  * Hypertext reference with a defined relationship.
  *
  * @param rel  the relationship to the current resource
  * @param href the referenced resource address
  */
final case class Link(rel: String, href: String)

object Link {

  /**
    * Constructs a [[Link]] from a relationship tag and a [[Uri]]
    *
    * @param rel the relationship to the current resource
    * @param uri the uri to reach that resource
    * @return an instance of [[Link]]
    */
  final def apply(rel: String, uri: Uri): Link =
    Link(rel, uri.toString())
}
