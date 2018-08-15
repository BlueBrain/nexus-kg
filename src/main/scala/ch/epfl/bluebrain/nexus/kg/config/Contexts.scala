package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Contexts {

  val base = url"https://bluebrain.github.io/nexus/contexts".value

  val errorCtxUri: AbsoluteIri    = base + "error"
  val tagCtxUri: AbsoluteIri      = base + "tag"
  val resourceCtxUri: AbsoluteIri = base + "resource"
  val resolverCtxUri: AbsoluteIri = base + "resolver"
  val viewCtxUri: AbsoluteIri     = base + "view"
  val shaclCtxUri: AbsoluteIri    = base + "shacl"
  val searchCtxUri: AbsoluteIri   = base + "search"

  val tagCtx: Json      = jsonContentOf("/contexts/tags-context.json")
  val resourceCtx: Json = jsonContentOf("/contexts/resource-context.json")
  val resolverCtx: Json = jsonContentOf("/contexts/resolver-context.json")
  val viewCtx: Json     = jsonContentOf("/contexts/view-context.json")
  val shaclCtx: Json    = jsonContentOf("/contexts/shacl-context.json")
  val searchCtx: Json   = jsonContentOf("/contexts/search-context.json")

}
