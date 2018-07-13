package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.vocabToAbsoluteUri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Schemas extends Resources {

  val schemas = "https://bluebrain.github.io/nexus/schemas"

  val shaclSchemaUri: AbsoluteIri         = url"$schemas/shacl"
  val crossResolverSchemaUri: AbsoluteIri = url"$schemas/cross-project-resolver"
  val resourceSchemaUri: AbsoluteIri      = url"$schemas/resource"
  val ontologySchemaUri: AbsoluteIri      = url"$schemas/ontology"

  val crossResolverSchema: Json = jsonContentOf("/schemas/cross-project-resolver.json")

}
