package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.file.File.Digest
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import io.circe.syntax._
import org.scalatest.{EitherValues, Matchers, WordSpec}

class DigestSpec extends WordSpec with Matchers with TestHelper with Randomness with EitherValues {

  private abstract class Ctx {
    val id = Id(ProjectRef(genUUID), genIri)
    def jsonDigest(digest: Digest, tpe: String = nxv.UpdateDigest.prefix): Json =
      Json.obj("@id"       -> id.value.asString.asJson,
               "@type"     -> tpe.asJson,
               "value"     -> digest.value.asJson,
               "algorithm" -> digest.algorithm.asJson)

  }

  "A Digest" should {

    "be converted to digest case class correctly" in new Ctx {
      val digest = Digest("SHA-256", genString())
      Digest(id, jsonDigest(digest)).right.value shouldEqual digest
    }

    "reject when algorithm is missing" in new Ctx {
      val digest = Digest("SHA-256", genString())
      Digest(id, jsonDigest(digest).removeKeys("algorithm")).left.value shouldEqual
        InvalidResourceFormat(id.ref, "'algorithm' field does not have the right format.")
    }

    "reject when value is missing" in new Ctx {
      val digest = Digest("SHA-256", genString())
      Digest(id, jsonDigest(digest).removeKeys("value")).left.value shouldEqual
        InvalidResourceFormat(id.ref, "'value' field does not have the right format.")
    }

    "reject when value is empty" in new Ctx {
      val digest = Digest("SHA-256", "")
      Digest(id, jsonDigest(digest)).left.value shouldEqual
        InvalidResourceFormat(id.ref, "'value' field does not have the right format.")
    }

    "reject when algorithm is empty" in new Ctx {
      val digest = Digest("", genString())
      Digest(id, jsonDigest(digest)).left.value shouldEqual
        InvalidResourceFormat(id.ref, "'algorithm' field does not have the right format.")
    }

    "reject when algorithm is invalid" in new Ctx {
      val digest = Digest(genString(), genString())
      Digest(id, jsonDigest(digest)).left.value shouldEqual
        InvalidResourceFormat(id.ref, "'algorithm' field does not have the right format.")
    }

    "reject when @type is missing" in new Ctx {
      val digest = Digest(genString(), genString())
      Digest(id, jsonDigest(digest).removeKeys("@type")).left.value shouldEqual
        InvalidResourceFormat(id.ref, "'@type' field does not have the right format.")
    }
    "reject when @type is invalid" in new Ctx {
      val digest = Digest(genString(), genString())
      Digest(id, jsonDigest(digest, tpe = genIri.asString)).left.value shouldEqual
        InvalidResourceFormat(id.ref, "'@type' field does not have the right format.")
    }
  }

}
