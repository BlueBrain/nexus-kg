package ch.epfl.bluebrain.nexus.kg.config

import cats.Id
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.apache.jena.rdf.model.Model

object Schemas {

  val base = url"https://bluebrain.github.io/nexus/schemas/".value

  //Schema URIs
  val shaclSchemaUri: AbsoluteIri         = base + "shacl-20170720.ttl"
  val resolverSchemaUri: AbsoluteIri      = base + "resolver.json"
  val unconstrainedSchemaUri: AbsoluteIri = base + "unconstrained.json"
  val fileSchemaUri: AbsoluteIri          = base + "file.json"
  val viewSchemaUri: AbsoluteIri          = base + "view.json"
  val storageSchemaUri: AbsoluteIri       = base + "storage.json"
  val ontologySchemaUri: AbsoluteIri      = base + "ontology.json"

  //Schema payloads
  val resolverSchema: Json = jsonContentOf("/schemas/resolver.json")
  val viewSchema: Json     = jsonContentOf("/schemas/view.json")
  val storageSchema: Json  = jsonContentOf("/schemas/storage.json")

  //Schema models
  val resolverSchemaModel: Id[Model] = resolveSchema(resolverSchema).asGraph(resolverSchemaUri).toOption.get.as[Model]()
  val viewSchemaModel: Id[Model]     = resolveSchema(viewSchema).asGraph(viewSchemaUri).toOption.get.as[Model]()
  val storageSchemaModel: Id[Model]  = resolveSchema(storageSchema).asGraph(storageSchemaUri).toOption.get.as[Model]()

  // Schema references
  val viewRef          = viewSchemaUri.ref
  val storageRef       = storageSchemaUri.ref
  val resolverRef      = resolverSchemaUri.ref
  val unconstrainedRef = unconstrainedSchemaUri.ref
  val shaclRef         = shaclSchemaUri.ref
  val fileRef          = fileSchemaUri.ref

  private def resolveSchema(schema: Json): Json =
    schema.replaceContext(schema.removeContextIris.appendContextOf(shaclCtx))

}
