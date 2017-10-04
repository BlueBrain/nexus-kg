package ch.epfl.bluebrain.nexus.kg.core.schemas

import cats.instances.try_._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.common.test.Randomness
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclSchema
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidatorErr.{CouldNotFindImports, IllegalImportDefinition}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaImportResolverSpec._
import io.circe.Json
import io.circe.parser._
import org.scalatest.{Matchers, TryValues, WordSpecLike}

import scala.util.{Success, Try}

class SchemaImportResolverSpec extends WordSpecLike with Matchers with TryValues with Randomness {

  private val baseUri = "http://localhost:8080/v0"
  private val context = parse(
    s"""
       |{
       |  "@context": {
       |    "owl" : "http://www.w3.org/2002/07/owl#",
       |    "imports" : {
       |      "@id" : "owl:imports",
       |      "@type" : "@id",
       |      "@container" : "@set"
       |    }
       |  },
       |  "shapes": []
       |}
     """.stripMargin).toTry.get

  private def jsonImportsSchema(id: SchemaId, imports: List[Json], published: Boolean = true): Schema = {
    val importsList = Json.fromValues(imports)
    val justImports = Json.obj("imports" -> importsList)
    val innerId = Json.obj("@id" -> Json.fromString(s"$baseUri/schemas/${id.show}"))
    val value = context deepMerge justImports deepMerge innerId
    Schema(id, 2L, value, deprecated = false, published = published)
  }

  private def uncheckedImportsSchema(id: SchemaId, imports: List[String], published: Boolean = true): Schema =
    jsonImportsSchema(id, imports.map(i => Json.fromString(i)), published)

  private def schema(id: SchemaId, imports: List[SchemaId] = Nil, published: Boolean = true): Schema =
    uncheckedImportsSchema(id, imports.map(i => s"$baseUri/schemas/${i.show}"), published)

  private def genSchemaId(): SchemaId =
    SchemaId(DomainId(OrgId("org"), "dom"), genString(), Version(1, 0, 0))

  "A SchemaImportResolver" should {
    "return no imports" in {
      val resolver = SchemaImportResolver[Try](baseUri, _ => Success(Some(schema(genSchemaId(), List(genSchemaId())))))
      resolver(schema(genSchemaId()).asShacl).success.value shouldBe empty
    }

    val leaf1 = schema(genSchemaId())
    val leaf2 = schema(genSchemaId())
    val mid1 = schema(genSchemaId(), List(leaf1.id, leaf2.id))
    val mid2 = schema(genSchemaId(), List(leaf1.id))
    val unpublished1 = schema(genSchemaId(), published = false)
    val unpublished2 = schema(genSchemaId(), published = false)
    val main = schema(genSchemaId(), List(mid1.id, mid2.id))

    "lookup imports transitively" in {
      val all = List(leaf1, leaf2, mid1, mid2, main)
      val loader = (id: SchemaId) => Success(all.find(_.id == id))
      val resolver = SchemaImportResolver[Try](baseUri, loader)
      resolver(main.asShacl).success.value.size shouldEqual 4
    }

    "aggregate missing imports" in {
      val all = List(mid1, mid2, main)
      val loader = (id: SchemaId) => Success(all.find(_.id == id))
      val resolver = SchemaImportResolver[Try](baseUri, loader)
      resolver(main.asShacl).failure.exception shouldEqual CouldNotFindImports(Set(
        s"$baseUri/schemas/${leaf1.id.show}",
        s"$baseUri/schemas/${leaf2.id.show}"
      ))
    }

    "aggregate single batch missing imports" in {
      val all = List(leaf1, mid1, main)
      val loader = (id: SchemaId) => Success(all.find(_.id == id))
      val resolver = SchemaImportResolver[Try](baseUri, loader)
      resolver(main.asShacl).failure.exception shouldEqual CouldNotFindImports(Set(
        s"$baseUri/schemas/${mid2.id.show}"
      ))
    }

    "aggregate unknown imports" in {
      val withUnknown = uncheckedImportsSchema(genSchemaId(), List(
        "http://localhost/a",
        "http://localhost/b",
        s"$baseUri/schemas/${mid1.id.show}"))
      val all = List(mid1)
      val loader = (id: SchemaId) => Success(all.find(_.id == id))
      val resolver = SchemaImportResolver[Try](baseUri, loader)
      resolver(withUnknown.asShacl).failure.exception shouldEqual IllegalImportDefinition(Set(
        "http://localhost/a", "http://localhost/b"
      ))
    }

    "ignore incorrectly typed imports" in {
      val withUnknown = jsonImportsSchema(genSchemaId(), List(Json.fromInt(12), Json.fromString("13")))
      val loader = (_: SchemaId) => Success(None)
      val resolver = SchemaImportResolver[Try](baseUri, loader)
      resolver(withUnknown.asShacl).success.value.size shouldEqual 0
    }

    "treat unpublished schemas as not found" in {
      val withUnpublished = schema(genSchemaId(), List(unpublished1.id, unpublished2.id, leaf1.id))
      val all = List(unpublished1, unpublished2, leaf1)
      val loader = (id: SchemaId) => Success(all.find(_.id == id))
      val resolver = SchemaImportResolver[Try](baseUri, loader)
      resolver(withUnpublished.asShacl).failure.exception shouldEqual CouldNotFindImports(Set(
        s"$baseUri/schemas/${unpublished1.id.show}",
        s"$baseUri/schemas/${unpublished2.id.show}"
      ))
    }
  }
}

object SchemaImportResolverSpec {
  implicit class AsShacl(schema: Schema) {
    def asShacl: ShaclSchema =
      ShaclSchema(schema.value)
  }
}