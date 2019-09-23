package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Contexts {

  val base = url"https://bluebrain.github.io/nexus/contexts/".value

  val errorCtxUri: AbsoluteIri      = base + "error.json"
  val tagCtxUri: AbsoluteIri        = base + "tag.json"
  val archiveCtxUri: AbsoluteIri    = base + "archive.json"
  val digestCtxUri: AbsoluteIri     = base + "digest.json"
  val statisticsCtxUri: AbsoluteIri = base + "statistics.json"
  val resourceCtxUri: AbsoluteIri   = base + "resource.json"
  val resolverCtxUri: AbsoluteIri   = base + "resolver.json"
  val viewCtxUri: AbsoluteIri       = base + "view.json"
  val storageCtxUri: AbsoluteIri    = base + "storage.json"
  val shaclCtxUri: AbsoluteIri      = base + "shacl-20170720.json"
  val searchCtxUri: AbsoluteIri     = base + "search.json"

  val tagCtx: Json        = jsonContentOf("/contexts/tags-context.json")
  val archiveCtx: Json    = jsonContentOf("/contexts/archive-context.json")
  val digestCtx: Json     = jsonContentOf("/contexts/digest-context.json")
  val statisticsCtx: Json = jsonContentOf("/contexts/statistics-context.json")
  val resourceCtx: Json   = jsonContentOf("/contexts/resource-context.json")
  val resolverCtx: Json   = jsonContentOf("/contexts/resolver-context.json")
  val viewCtx: Json       = jsonContentOf("/contexts/view-context.json")
  val storageCtx: Json    = jsonContentOf("/contexts/storage-context.json")
  val shaclCtx: Json      = jsonContentOf("/contexts/shacl-context.json")
  val searchCtx: Json     = jsonContentOf("/contexts/search-context.json")

}
