package ch.epfl.bluebrain.nexus.kg.resources

import akka.http.scaladsl.model.ContentType
import akka.http.scaladsl.model.Uri.Path
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.file.File.LinkDescription
import io.circe.Json
import org.scalatest.{EitherValues, Matchers, WordSpec}
import io.circe.syntax._
import ch.epfl.bluebrain.nexus.rdf.syntax._

class LinkDescriptionSpec extends WordSpec with Matchers with TestHelper with Randomness with EitherValues {

  private abstract class Ctx {
    val id = Id(ProjectRef(genUUID), genIri)
    val p  = genString() + "/" + genString()
    val f  = genString()
    val m  = "application/json"
    def jsonLink(mediaType: String = m, filename: String = f, path: String = p): Json =
      Json.obj("filename" -> filename.asJson, "path" -> path.asJson, "mediaType" -> mediaType.asJson)

  }

  "A Link Description" should {

    "be converted to link description case class correctly" in new Ctx {
      LinkDescription(id, jsonLink()).right.value shouldEqual
        LinkDescription(Path(p), f, ContentType.parse(m).right.value)
    }

    "reject when filename is missing" in new Ctx {
      LinkDescription(id, jsonLink().removeKeys("filename")).left.value shouldBe a[InvalidResourceFormat]
    }

    "reject when filename is empty" in new Ctx {
      LinkDescription(id, jsonLink(filename = "")).left.value shouldBe a[InvalidResourceFormat]
    }

    "reject when mediaType does not exist" in new Ctx {
      LinkDescription(id, jsonLink().removeKeys("mediaType")).left.value shouldBe a[InvalidResourceFormat]
    }

    "reject when mediaType has the wrong format" in new Ctx {
      LinkDescription(id, jsonLink(mediaType = genString())).left.value shouldBe a[InvalidResourceFormat]
    }

    "reject when path does not exist" in new Ctx {
      LinkDescription(id, jsonLink().removeKeys("path")).left.value shouldBe a[InvalidResourceFormat]
    }

    "reject when path is empty" in new Ctx {
      LinkDescription(id, jsonLink(path = "")).left.value shouldBe a[InvalidResourceFormat]
    }
  }
}
