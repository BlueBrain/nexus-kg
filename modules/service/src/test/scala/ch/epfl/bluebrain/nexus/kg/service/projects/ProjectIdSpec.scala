package ch.epfl.bluebrain.nexus.kg.service.projects

import cats.syntax.show._
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

import scala.util.Left

class ProjectIdSpec extends WordSpecLike with Matchers with Inspectors {

  "A ProjectId" should {

    "create project ids from apply" in {
      ProjectId("project") shouldEqual Some(new ProjectId("project"))
      ProjectId("#&") shouldEqual None
    }

    "print projectId with show" in {
      ProjectId("project").get.show shouldEqual "project"
    }

    "be encoded properly into json" in {
      ProjectId("project").get.asJson.noSpaces shouldEqual """"project""""
    }

    "be decoded properly from json" in {
      decode[ProjectId](""""project"""") shouldEqual Right(ProjectId("project").get)
    }

    "fail to decode" in {
      forAll(List("/", "/asd", "asd/", "asd/ads/asd", "#$56")) { str =>
        decode[ProjectId](s""""$str"""") shouldBe a[Left[_, _]]
      }
    }
  }
}
