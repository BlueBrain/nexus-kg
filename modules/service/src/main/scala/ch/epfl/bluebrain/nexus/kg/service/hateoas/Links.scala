package ch.epfl.bluebrain.nexus.kg.service.hateoas

import akka.http.scaladsl.model.Uri
import io.circe.{Encoder, Json}
import io.circe.syntax._

/**
  * Data type which wraps the discoverability relationships.
  * @param values Map of key pairs where key is the relationship ''rel'' and value is the links ''href'' to the relationship ''rel''
  */
final case class Links private (values: Map[String, List[Uri]]) {

  /**
    * Adds a relationship ''rel'' with links ''href''.
    *
    * @param rel  the relationship predicate
    * @param href the relationship link
    */
  def +(rel: String, href: Uri*): Links = Links(values + (rel -> href.toList))

  /**I
    * Merges two [[Links]] together and returns a new [[Links]] with the added elements.
    *
    * @param links the [[Links]] we want to add to the current instance
    */
  def ++(links: Links): Links = Links(values ++ links.values)

  /**
    * Merges two [[Links]] together. Returns a new [[Links]] with the added elements if the provided ''links'' are defined, or the current [[Links]] otherwise.
    *
    * @param links the optionally provided [[Links]] we want to add to the current instance
    */
  def ++(links: Option[Links]): Links = links.map(this ++ _).getOrElse(this)

  /**
    * Fetches the specific link ''href'' for the provided relationship ''rel''
    *
    * @param rel the relationship value
    * @return an option value containing the value associated with `rel` in this links,
    *         or `None` if none exists.
    */
  def get(rel: String): Option[List[Uri]] = values.get(rel)

}
object Links {

  /**
    * Constructs [[Links]] from a number of [[Tuple2]]
    *
    * @param values the key pairs of ''rel'' and ''href''
    */
  final def apply(values: (String, Uri)*): Links =
    Links(values.groupBy(_._1).map {
      case (rel, hrefs) => rel -> hrefs.toList.map(_._2)
    })

  implicit val encoder: Encoder[Links] =
    Encoder.encodeJson.contramap { links =>
      links.values.map {
        case (rel, href :: List()) => rel -> Json.fromString(s"$href")
        case (rel, hrefs)          => rel -> Json.arr(hrefs.map(href => Json.fromString(s"$href")): _*)
      }.asJson
    }

}
