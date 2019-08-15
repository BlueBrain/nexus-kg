package ch.epfl.bluebrain.nexus.kg.resources

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{ContentType, ContentTypes}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{FileDescription, LinkDescription}
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import io.circe.syntax._
import org.scalatest.{EitherValues, Matchers, WordSpec}

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

    "be decoded correctly" in new Ctx {
      LinkDescription(id, jsonLink()).right.value shouldEqual
        LinkDescription(Path(p), Some(f), ContentType.parse(m).toOption)

      LinkDescription(id, Json.obj("path" -> p.asJson)).right.value shouldEqual
        LinkDescription(Path(p), None, None)
    }

    "accept missing filename" in new Ctx {
      LinkDescription(id, jsonLink().removeKeys("filename")).right.value shouldEqual
        LinkDescription(Path(p), None, ContentType.parse(m).toOption)
    }

    "reject empty filename" in new Ctx {
      LinkDescription(id, jsonLink(filename = "")).left.value shouldBe a[InvalidResourceFormat]
    }

    "accept missing mediaType" in new Ctx {
      LinkDescription(id, jsonLink().removeKeys("mediaType")).right.value shouldEqual
        LinkDescription(Path(p), Some(f), None)
    }

    "reject wrong mediaType format" in new Ctx {
      LinkDescription(id, jsonLink(mediaType = genString())).left.value shouldBe a[InvalidResourceFormat]
    }

    "reject missing path" in new Ctx {
      LinkDescription(id, jsonLink().removeKeys("path")).left.value shouldBe a[InvalidResourceFormat]
    }

    "be converted to a FileDescription correctly" in new Ctx {
      val fileDesc1 = FileDescription.from(LinkDescription(Path("/foo/bar/file.ext"), None, None))
      fileDesc1.filename shouldEqual "file.ext"
      fileDesc1.mediaType shouldEqual ContentTypes.`application/octet-stream`

      val fileDesc2 =
        FileDescription.from(LinkDescription(Path("/foo/bar/somedir/"), None, ContentType.parse(m).toOption))
      fileDesc2.filename shouldEqual "somedir"
      fileDesc2.mediaType shouldEqual ContentTypes.`application/json`

      val fileDesc3 =
        FileDescription.from(LinkDescription(Path("/foo/bar/baz"), Some("file.json"), ContentType.parse(m).toOption))
      fileDesc3.filename shouldEqual "file.json"
      fileDesc3.mediaType shouldEqual ContentTypes.`application/json`
    }
  }
}
