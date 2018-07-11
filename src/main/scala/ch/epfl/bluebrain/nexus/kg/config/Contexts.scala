package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.toAbsoluteUri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Contexts extends Resources {

  val contexts = "https://bluebrain.github.io/nexus/contexts"

  val errorCtxUri: AbsoluteIri    = url"$contexts/error"
  val tagCtxUri: AbsoluteIri      = url"$contexts/tag"
  val resourceCtxUri: AbsoluteIri = url"$contexts/resource"
  val resolverCtxUri: AbsoluteIri = url"$contexts/resolver"
  val shaclCtxUri: AbsoluteIri    = url"$contexts/shacl"
  val searchCtxUri: AbsoluteIri   = url"$contexts/search"

  val tagCtx: Json      = jsonContentOf("/contexts/tags-context.json")
  val resourceCtx: Json = jsonContentOf("/contexts/resource-context.json")
  val resolverCtx: Json = jsonContentOf("/contexts/resolver-context.json")
  val shaclCtx: Json    = jsonContentOf("/contexts/shacl-context.json")

}
