package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.vocabToAbsoluteUri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Schemas {

  val schemas = "https://bluebrain.github.io/nexus/schemas"

  val shaclSchemaUri: AbsoluteIri    = url"$schemas/shacl"
  val resolverSchemaUri: AbsoluteIri = url"$schemas/resolver"
  val resourceSchemaUri: AbsoluteIri = url"$schemas/resource"
  val ontologySchemaUri: AbsoluteIri = url"$schemas/ontology"

  val resolverSchema: Json = jsonContentOf("/schemas/resolver.json")

}
