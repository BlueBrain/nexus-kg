package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Contexts {

  val base = url"https://bluebrain.github.io/nexus/contexts/".value

  val errorCtxUri: AbsoluteIri    = base + "error,json"
  val tagCtxUri: AbsoluteIri      = base + "tag.json"
  val resourceCtxUri: AbsoluteIri = base + "resource.json"
  val resolverCtxUri: AbsoluteIri = base + "resolver.json"
  val viewCtxUri: AbsoluteIri     = base + "view.json"
  val shaclCtxUri: AbsoluteIri    = base + "shacl-20170720.json"
  val searchCtxUri: AbsoluteIri   = base + "search.json"

  val tagCtx: Json      = jsonContentOf("/contexts/tags-context.json")
  val resourceCtx: Json = jsonContentOf("/contexts/resource-context.json")
  val resolverCtx: Json = jsonContentOf("/contexts/resolver-context.json")
  val viewCtx: Json     = jsonContentOf("/contexts/view-context.json")
  val shaclCtx: Json    = jsonContentOf("/contexts/shacl-context.json")
  val searchCtx: Json   = jsonContentOf("/contexts/search-context.json")

}
