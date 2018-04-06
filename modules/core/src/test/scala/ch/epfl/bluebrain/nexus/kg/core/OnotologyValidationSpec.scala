package ch.epfl.bluebrain.nexus.kg.core

import java.util.regex.Pattern.quote
import cats.instances.try_._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.{ShaclSchema, ShaclValidator}
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, InstanceImportResolver}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaImportResolver
import io.circe.Json
import org.scalatest.{Matchers, TryValues, WordSpecLike}

import scala.util.{Success, Try}

class OnotologyValidationSpec extends WordSpecLike with Matchers with Resources with TryValues {

  private val baseUri = "http://localhost:8080/v0"

  private val replacements = Map(quote("{{base}}") -> baseUri)
  private val ontology     = jsonContentOf("/resolver/data/ontology.json", replacements)
  private val schema       = jsonContentOf("/resolver/schemas/schema.json", replacements)

  private val ctxResolver = (_: Json) => Try(Json.obj())

  private val instanceResolver = (id: InstanceId) => Success(Some(Instance(id, 1L, ontology, deprecated = false)))

  private val schemaImportResolver = new SchemaImportResolver[Try](baseUri, _ => Try(None), ctxResolver)
  private val instanceImportResolver =
    new InstanceImportResolver[Try](baseUri, instanceResolver, ctxResolver)

  private val validator: ShaclValidator[Try] =
    new ShaclValidator[Try](AggregatedImportResolver(schemaImportResolver, instanceImportResolver))

  "An OntologyValidation" should {

    "validate correctly an schema which uses an ontology" in {
      val data = jsonContentOf("/resolver/data/data-correct.json", replacements)
      validator(ShaclSchema(schema), ontology, data).success.value.conforms shouldEqual true
    }

    "validate incorrectly an schema which uses an ontology" in {
      val data = jsonContentOf("/resolver/data/data-incorrect.json", replacements)
      validator(ShaclSchema(schema), ontology, data).success.value.conforms shouldEqual false

    }
  }

}
