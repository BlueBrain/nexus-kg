package ch.epfl.bluebrain.nexus.kg.core.instances

import java.util.UUID

import cats.instances.try_._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclSchema
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidatorErr.{CouldNotFindImports, IllegalImportDefinition}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceImportResolverSpec._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import io.circe.Json
import io.circe.parser._
import org.scalatest.{Matchers, TryValues, WordSpecLike}

import scala.util.{Success, Try}

class InstanceImportResolverSpec extends WordSpecLike with Matchers with TryValues with Randomness {

  private val baseUri         = "http://localhost:8080/v0"
  private val resolvedContext = parse(s"""
       |{
       |  "@context": {
       |    "owl": "http://www.w3.org/2002/07/owl#",
       |    "imports": {
       |      "@id": "owl:imports",
       |      "@type": "@id",
       |      "@container": "@set"
       |    }
       |  }
       |}
     """.stripMargin).toTry.get
  private val context         = parse(s"""
       |{
       |  "@context": "http://127.0.0.1:8080/v0/contexts/test/test/test/v0.1.0",
       |  "shapes": []
       |}
     """.stripMargin).toTry.get

  private def jsonImportsInstance(id: InstanceId, imports: List[Json], resolveContext: Boolean): Instance = {
    val importsList = Json.fromValues(imports)
    val justImports = Json.obj("imports" -> importsList)
    val innerId =
      Json.obj("@id" -> Json.fromString(s"$baseUri/data/${id.show}"))
    val value =
      if (resolveContext) contextResolver(context deepMerge justImports deepMerge innerId).get
      else context deepMerge justImports deepMerge innerId
    Instance(id, 2L, value, deprecated = false)
  }

  private def uncheckedImportsInstance(id: InstanceId, imports: List[String], resolveContext: Boolean): Instance =
    jsonImportsInstance(id, imports.map(i => Json.fromString(i)), resolveContext)

  private def instance(id: InstanceId, imports: List[InstanceId] = Nil, resolveContext: Boolean = false): Instance =
    uncheckedImportsInstance(id, imports.map(i => s"$baseUri/data/${i.show}"), resolveContext)

  private def genInstanceId(): InstanceId =
    InstanceId(SchemaId(DomainId(OrgId("org"), "dom"), genString(), Version(1, 0, 0)),
               UUID.randomUUID().toString.toLowerCase)

  def contextResolver(json: Json): Try[Json] =
    Success(json deepMerge resolvedContext)

  "A InstanceImportResolver" should {
    "return no imports" in {
      val resolver =
        new InstanceImportResolver[Try](baseUri,
                                        _ => Success(Some(instance(genInstanceId(), List(genInstanceId())))),
                                        contextResolver)
      resolver(instance(genInstanceId(), resolveContext = true).asShacl).success.value shouldBe empty
    }

    val leaf1 = instance(genInstanceId())
    val leaf2 = instance(genInstanceId())
    val mid1  = instance(genInstanceId(), List(leaf1.id, leaf2.id))
    val mid2  = instance(genInstanceId(), List(leaf1.id))
    val main  = instance(genInstanceId(), List(mid1.id, mid2.id), resolveContext = true)

    "lookup imports transitively" in {
      val all      = List(leaf1, leaf2, mid1, mid2, main)
      val loader   = (id: InstanceId) => Success(all.find(_.id == id))
      val resolver = new InstanceImportResolver[Try](baseUri, loader, contextResolver)
      resolver(main.asShacl).success.value.size shouldEqual 4
    }

    "aggregate missing imports" in {
      val all      = List(mid1, mid2, main)
      val loader   = (id: InstanceId) => Success(all.find(_.id == id))
      val resolver = new InstanceImportResolver[Try](baseUri, loader, contextResolver)
      resolver(main.asShacl).failure.exception shouldEqual CouldNotFindImports(
        Set(
          s"$baseUri/data/${leaf1.id.show}",
          s"$baseUri/data/${leaf2.id.show}"
        ))
    }

    "aggregate single batch missing imports" in {
      val all      = List(leaf1, mid1, main)
      val loader   = (id: InstanceId) => Success(all.find(_.id == id))
      val resolver = new InstanceImportResolver[Try](baseUri, loader, contextResolver)
      resolver(main.asShacl).failure.exception shouldEqual CouldNotFindImports(
        Set(
          s"$baseUri/data/${mid2.id.show}"
        ))
    }

    "aggregate unknown imports" in {
      val withUnknown =
        uncheckedImportsInstance(genInstanceId(),
                                 List("http://localhost/a", "http://localhost/b", s"$baseUri/data/${mid1.id.show}"),
                                 resolveContext = true)
      val all      = List(mid1)
      val loader   = (id: InstanceId) => Success(all.find(_.id == id))
      val resolver = new InstanceImportResolver[Try](baseUri, loader, contextResolver)
      resolver(withUnknown.asShacl).failure.exception shouldEqual IllegalImportDefinition(
        Set(
          "http://localhost/a",
          "http://localhost/b"
        ))
    }

    "ignore incorrectly typed imports" in {
      val withUnknown =
        jsonImportsInstance(genInstanceId(), List(Json.fromInt(12), Json.fromString("13")), resolveContext = true)
      val loader   = (_: InstanceId) => Success(None)
      val resolver = new InstanceImportResolver[Try](baseUri, loader, contextResolver)
      resolver(withUnknown.asShacl).success.value.size shouldEqual 0
    }
  }
}

object InstanceImportResolverSpec {
  implicit class AsShacl(instance: Instance) {
    def asShacl: ShaclSchema =
      ShaclSchema(instance.value)
  }
}
