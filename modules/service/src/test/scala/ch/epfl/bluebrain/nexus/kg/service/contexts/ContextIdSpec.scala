package ch.epfl.bluebrain.nexus.kg.service.contexts

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectId
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.util.Left

class ContextIdSpec extends WordSpecLike with Matchers with Inspectors {

  "A ContextId" should {
    val projectId       = ProjectId("abc").get
    val contextId       = ContextId(projectId, "name").get
    val contextIdString = """"abc/name""""

    "create context ids from apply" in {
      ContextId("project/context") shouldEqual Some(new ContextId(ProjectId("project").get, "context"))
      ContextId(projectId, "#&") shouldEqual None
      ContextId("some/#&") shouldEqual None
    }

    "print schemaId with show" in {
      new ContextId(ProjectId("project").get, "context").show shouldEqual "project/context"
    }

    "be encoded properly into json" in {
      contextId.asJson.noSpaces shouldEqual contextIdString
    }

    "be decoded properly from json" in {
      decode[ContextId](contextIdString) shouldEqual Right(contextId)
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
        decode[ContextId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}
