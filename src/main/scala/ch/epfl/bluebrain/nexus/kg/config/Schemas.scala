package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Schemas {

  val base = url"https://bluebrain.github.io/nexus/schemas/".value

  val shaclSchemaUri: AbsoluteIri    = base + "shacl-20170720.ttl"
  val resolverSchemaUri: AbsoluteIri = base + "resolver.json"
  val resourceSchemaUri: AbsoluteIri = base + "resource.json"
  val fileSchemaUri: AbsoluteIri     = base + "file.json"
  val viewSchemaUri: AbsoluteIri     = base + "view.json"
  val ontologySchemaUri: AbsoluteIri = base + "ontology.json"

  val resolverSchema: Json = jsonContentOf("/schemas/resolver.json")
  val viewSchema: Json     = jsonContentOf("/schemas/view.json")

  val viewRef     = viewSchemaUri.ref
  val resolverRef = resolverSchemaUri.ref
  val resourceRef = resourceSchemaUri.ref
  val shaclRef    = shaclSchemaUri.ref
  val fileRef     = fileSchemaUri.ref

}
