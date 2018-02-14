package ch.epfl.bluebrain.nexus.kg.service.schemas

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectId
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.util.Left

class SchemaIdSpec extends WordSpecLike with Matchers with Inspectors {

  "A SchemaId" should {
    val projectId      = ProjectId("abc").get
    val schemaId       = SchemaId(projectId, "name").get
    val schemaIdString = """"abc/name""""

    "create schema ids from apply" in {
      SchemaId("project/schema") shouldEqual Some(new SchemaId(ProjectId("project").get, "schema"))
      SchemaId(projectId, "#&") shouldEqual None
      SchemaId("some/#&") shouldEqual None
    }

    "print schemaId with show" in {
      new SchemaId(ProjectId("project").get, "schema").show shouldEqual "project/schema"
    }

    "be encoded properly into json" in {
      schemaId.asJson.noSpaces shouldEqual schemaIdString
    }

    "be decoded properly from json" in {
      decode[SchemaId](schemaIdString) shouldEqual Right(schemaId)
    }

    "fail to decode" in {
      forAll(
        List("asd",
             "/",
             "/asd",
             "/asd/%$",
             "asd/",
             "asd/ads/asd",
             "asd/asd/asd/v1.1",
             "asd/asd/asd/v1.1.2.3",
             "asd/asd/asd/v1.1.a",
             "asd/asd/a d/v1.1.2")) { str =>
        decode[SchemaId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}
