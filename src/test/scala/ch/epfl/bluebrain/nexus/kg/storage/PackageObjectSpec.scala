package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.Path
import java.util.UUID

import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import org.scalatest.{FlatSpec, Matchers}

class PackageObjectSpec extends FlatSpec with Matchers {

  "uriToPath" should "convert a valid Akka Uri to a Path" in {
    uriToPath("file:///some/path") shouldEqual Some(Path.of("/some/path"))
    uriToPath("s3://some/path") shouldEqual None
    uriToPath("foo") shouldEqual None
  }

  "mangle" should "generate a properly mangled path given a file project and UUID" in {
    val projUuid = UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")
    val fileUuid = UUID.fromString("b1d7cda2-1ec0-40d2-b12e-3baf4895f7d7")
    mangle(ProjectRef(projUuid), fileUuid) shouldEqual
      "4947db1e-33d8-462b-9754-3e8ae74fcd4e/b/1/d/7/c/d/a/2/b1d7cda2-1ec0-40d2-b12e-3baf4895f7d7"
  }
}
