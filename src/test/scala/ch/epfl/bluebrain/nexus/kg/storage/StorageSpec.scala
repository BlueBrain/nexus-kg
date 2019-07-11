package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.Paths
import java.time.{Clock, Instant, ZoneId}
import java.util.regex.Pattern.quote

import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.test.{CirceEq, Resources}
import ch.epfl.bluebrain.nexus.iam.client.types.Permission
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.storage.Storage._
import ch.epfl.bluebrain.nexus.kg.storage.StorageEncoder._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, OptionValues, WordSpecLike}

class StorageSpec
    extends WordSpecLike
    with Matchers
    with OptionValues
    with Resources
    with TestHelper
    with Inspectors
    with CirceEq {
  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  val readDisk               = Permission.unsafe("disk/read")
  val writeDisk              = Permission.unsafe("disk/write")
  val readS3                 = Permission.unsafe("s3/read")
  val writeS3                = Permission.unsafe("s3/write")
  private implicit val storageConfig =
    StorageConfig(
      DiskStorageConfig(Paths.get("/tmp/"), "SHA-256", readDisk, writeDisk, false),
      RemoteDiskStorageConfig("http://example.com", None, "SHA-256", read, write, true),
      S3StorageConfig("MD5", readS3, writeS3, true),
      "password",
      "salt"
    )
  "A Storage" when {
    val iri        = url"http://example.com/id".value
    val projectRef = ProjectRef(genUUID)
    val id         = Id(projectRef, iri)

    val diskStorage = jsonContentOf("/storage/disk.json").appendContextOf(storageCtx)
    val s3Storage   = jsonContentOf("/storage/s3.json").appendContextOf(storageCtx)
    val remoteDiskStorage =
      jsonContentOf("/storage/remoteDisk.json",
                    Map(quote("{read}")   -> "myRead",
                        quote("{write}")  -> "myWrite",
                        quote("{folder}") -> "folder",
                        quote("{cred}")   -> "credentials"))
        .appendContextOf(storageCtx)

    "constructing" should {
      val diskStoragePerms =
        jsonContentOf("/storage/diskPerms.json", Map(quote("{read}") -> "myRead", quote("{write}") -> "myWrite"))
          .appendContextOf(storageCtx)
      val s3Minimal = jsonContentOf("/storage/s3-minimal.json").appendContextOf(storageCtx)

      "return a DiskStorage" in {
        val resource = simpleV(id, diskStorage, types = Set(nxv.Storage, nxv.DiskStorage))
        Storage(resource, encrypt = false).right.value shouldEqual
          DiskStorage(projectRef, iri, 1L, false, false, "SHA-256", Paths.get("/tmp"), readDisk, writeDisk)
      }

      "return a DiskStorage with custom readPermission and writePermission" in {
        val resource      = simpleV(id, diskStoragePerms, types = Set(nxv.Storage, nxv.DiskStorage))
        val expectedRead  = Permission.unsafe("myRead")
        val expectedWrite = Permission.unsafe("myWrite")
        Storage(resource, encrypt = false).right.value shouldEqual
          DiskStorage(projectRef, iri, 1L, false, false, "SHA-256", Paths.get("/tmp"), expectedRead, expectedWrite)
      }

      "return a RemoteDiskStorage" in {
        val resource      = simpleV(id, remoteDiskStorage, types = Set(nxv.Storage, nxv.RemoteDiskStorage))
        val expectedRead  = Permission.unsafe("myRead")
        val expectedWrite = Permission.unsafe("myWrite")
        Storage(resource, encrypt = false).right.value shouldEqual
          RemoteDiskStorage(projectRef,
                            iri,
                            1L,
                            false,
                            false,
                            "SHA-256",
                            "http://example.com/some",
                            Some("credentials"),
                            "folder",
                            expectedRead,
                            expectedWrite)
      }

      "return a RemoteDiskStorage encrypted" in {
        val resource      = simpleV(id, remoteDiskStorage, types = Set(nxv.Storage, nxv.RemoteDiskStorage))
        val expectedRead  = Permission.unsafe("myRead")
        val expectedWrite = Permission.unsafe("myWrite")
        Storage(resource, encrypt = true).right.value shouldEqual
          RemoteDiskStorage(projectRef,
                            iri,
                            1L,
                            false,
                            false,
                            "SHA-256",
                            "http://example.com/some",
                            Some("credentials".encrypt),
                            "folder",
                            expectedRead,
                            expectedWrite)
      }

      "return an S3Storage" in {
        val resource = simpleV(id, s3Minimal, types = Set(nxv.Storage, nxv.S3Storage))

        Storage(resource, encrypt = false).right.value shouldEqual
          S3Storage(projectRef, iri, 1L, false, true, "MD5", "bucket", S3Settings(None, None, None), readS3, writeS3)
      }

      "return an S3Storage with credentials and region" in {
        val resource      = simpleV(id, s3Storage, types = Set(nxv.Storage, nxv.S3Storage))
        val settings      = S3Settings(Some(S3Credentials("access", "secret")), Some("endpoint"), Some("region"))
        val expectedRead  = Permission.unsafe("my/read")
        val expectedWrite = Permission.unsafe("my/write")
        Storage(resource, encrypt = false).right.value shouldEqual
          S3Storage(projectRef, iri, 1L, false, true, "MD5", "bucket", settings, expectedRead, expectedWrite)
      }

      "return an S3Storage with encrypted credentials" in {
        val resource = simpleV(id, s3Storage, types = Set(nxv.Storage, nxv.S3Storage))
        val settings = S3Settings(Some(S3Credentials("MrAw2AGFs3T/+2M6nxOsuQ==", "Qa76lYhMOK9GPyTrxK26Jg==")),
                                  Some("endpoint"),
                                  Some("region"))
        val expectedRead  = Permission.unsafe("my/read")
        val expectedWrite = Permission.unsafe("my/write")
        Storage(resource, encrypt = true).right.value shouldEqual
          S3Storage(projectRef, iri, 1L, false, true, "MD5", "bucket", settings, expectedRead, expectedWrite)
      }

      "fail on DiskStorage when types are wrong" in {
        val resource = simpleV(id, diskStorage, types = Set(nxv.Storage))
        Storage(resource, encrypt = false).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on RemoteDiskStorage when types are wrong" in {
        val resource = simpleV(id, remoteDiskStorage, types = Set(nxv.Storage))
        Storage(resource, encrypt = false).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on S3Storage when types are wrong" in {
        val resource = simpleV(id, s3Storage, types = Set(nxv.S3Storage))
        Storage(resource, encrypt = false).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on DiskStorage when required parameters are not present" in {
        val resource = simpleV(id, diskStorage.removeKeys("volume"), types = Set(nxv.Storage, nxv.DiskStorage))
        Storage(resource, encrypt = false).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on S3Storage when required parameters are not present" in {
        val resource = simpleV(id, s3Storage.removeKeys("default"), types = Set(nxv.Storage, nxv.S3Storage))
        Storage(resource, encrypt = false).left.value shouldBe a[InvalidResourceFormat]
      }
    }

    "converting into json " should {
      "return the json representation for storages" in {

        val expectedRead  = Permission.unsafe("myRead")
        val expectedWrite = Permission.unsafe("myWrite")
        // format: off
        val disk: Storage = DiskStorage(projectRef, iri, 1L, false, false, "SHA-256", Paths.get("/tmp"), readDisk, writeDisk)
        val s3: Storage = S3Storage(projectRef, iri, 1L, false, true, "MD5", "bucket", S3Settings(None, None, None), readS3, writeS3)
        val remote: Storage = RemoteDiskStorage(projectRef, iri, 1L, false, false, "SHA-256", "http://example.com/some", Some("credentials"), "folder", expectedRead, expectedWrite)
        // format: on

        forAll(
          List(disk   -> jsonContentOf("/storage/disk-meta.json"),
               s3     -> jsonContentOf("/storage/s3-meta.json"),
               remote -> jsonContentOf("/storage/remoteDisk-meta.json"))) {
          case (storage, expectedJson) =>
            val json = storage.as[Json](storageCtx.appendContextOf(resourceCtx)).right.value.removeKeys("@context")
            json should equalIgnoreArrayOrder(expectedJson.removeKeys("@context"))
        }
      }
    }
  }

}
