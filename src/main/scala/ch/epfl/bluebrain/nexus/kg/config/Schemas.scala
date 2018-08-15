package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Schemas {

  val base = url"https://bluebrain.github.io/nexus/schemas/".value

  val shaclSchemaUri: AbsoluteIri    = base + "shacl"
  val resolverSchemaUri: AbsoluteIri = base + "resolver"
  val resourceSchemaUri: AbsoluteIri = base + "resource"
  val viewSchemaUri: AbsoluteIri     = base + "view"
  val ontologySchemaUri: AbsoluteIri = base + "ontology"

  val resolverSchema: Json = jsonContentOf("/schemas/resolver.json")
  val viewSchema: Json     = jsonContentOf("/schemas/view.json")

}
