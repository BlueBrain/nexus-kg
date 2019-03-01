package ch.epfl.bluebrain.nexus.kg.config

import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json

object Schemas {

  val base = url"https://bluebrain.github.io/nexus/schemas/".value

  val shaclSchemaUri: AbsoluteIri         = base + "shacl-20170720.ttl"
  val resolverSchemaUri: AbsoluteIri      = base + "resolver.json"
  val unconstrainedSchemaUri: AbsoluteIri = base + "unconstrained.json"
  val fileSchemaUri: AbsoluteIri          = base + "file.json"
  val viewSchemaUri: AbsoluteIri          = base + "view.json"
  val storageSchemaUri: AbsoluteIri       = base + "storage.json"
  val ontologySchemaUri: AbsoluteIri      = base + "ontology.json"

  val resolverSchema: Json = jsonContentOf("/schemas/resolver.json")
  val viewSchema: Json     = jsonContentOf("/schemas/view.json")
  val storageSchema: Json  = jsonContentOf("/schemas/storage.json")

  val viewRef          = viewSchemaUri.ref
  val storageRef       = storageSchemaUri.ref
  val resolverRef      = resolverSchemaUri.ref
  val unconstrainedRef = unconstrainedSchemaUri.ref
  val shaclRef         = shaclSchemaUri.ref
  val fileRef          = fileSchemaUri.ref

}
