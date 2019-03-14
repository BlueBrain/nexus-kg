package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.Paths
import java.time.{Clock, Instant, ZoneId}
import java.util.regex.Pattern.quote

import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.iam.client.types.Permission
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{DiskStorageConfig, S3StorageConfig, StorageConfig}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.kg.storage.Storage._
import ch.epfl.bluebrain.nexus.kg.storage.StorageEncoder._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import org.scalatest.{Inspectors, Matchers, OptionValues, WordSpecLike}

class StorageSpec extends WordSpecLike with Matchers with OptionValues with Resources with TestHelper with Inspectors {
  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  val readDisk               = Permission.unsafe("disk-read")
  val writeDisk              = Permission.unsafe("disk-write")
  val readS3                 = Permission.unsafe("s3-read")
  val writeS3                = Permission.unsafe("s3-write")
  private implicit val storageConfig =
    StorageConfig(DiskStorageConfig(Paths.get("/tmp/"), "SHA-256", readDisk, writeDisk),
                  S3StorageConfig("MD5", readS3, writeS3))
  "A Storage" when {
    val iri        = url"http://example.com/id".value
    val projectRef = ProjectRef(genUUID)
    val id         = Id(projectRef, iri)

    "constructing" should {
      val diskStorage = jsonContentOf("/storage/disk.json").appendContextOf(storageCtx)
      val diskStoragePerms =
        jsonContentOf("/storage/diskPerms.json", Map(quote("{read}") -> "myRead", quote("{write}") -> "myWrite"))
          .appendContextOf(storageCtx)
      val s3Storage = jsonContentOf("/storage/s3.json").appendContextOf(storageCtx)
      val s3Minimal = jsonContentOf("/storage/s3-minimal.json").appendContextOf(storageCtx)

      "return a DiskStorage" in {
        val resource = simpleV(id, diskStorage, types = Set(nxv.Storage, nxv.DiskStorage))
        Storage(resource).right.value shouldEqual
          DiskStorage(projectRef, iri, 1L, false, false, "SHA-256", Paths.get("/tmp"), readDisk, writeDisk)
      }

      "return a DiskStorage with custom readPermission and writePermission" in {
        val resource      = simpleV(id, diskStoragePerms, types = Set(nxv.Storage, nxv.DiskStorage))
        val expectedRead  = Permission.unsafe("myRead")
        val expectedWrite = Permission.unsafe("myWrite")
        Storage(resource).right.value shouldEqual
          DiskStorage(projectRef, iri, 1L, false, false, "SHA-256", Paths.get("/tmp"), expectedRead, expectedWrite)
      }

      "return an S3Storage" in {
        val resource = simpleV(id, s3Minimal, types = Set(nxv.Storage, nxv.S3Storage, nxv.Alpha))

        Storage(resource).right.value shouldEqual
          S3Storage(projectRef, iri, 1L, false, true, "MD5", "bucket", S3Settings(None, None, None), readS3, writeS3)
      }

      "return an S3Storage with credentials and region" in {
        val resource      = simpleV(id, s3Storage, types = Set(nxv.Storage, nxv.S3Storage, nxv.Alpha))
        val settings      = S3Settings(Some(S3Credentials("access", "secret")), Some("endpoint"), Some("region"))
        val expectedRead  = Permission.unsafe("my/read")
        val expectedWrite = Permission.unsafe("my/write")
        Storage(resource).right.value shouldEqual
          S3Storage(projectRef, iri, 1L, false, true, "MD5", "bucket", settings, expectedRead, expectedWrite)
      }

      "fail on DiskStorage when types are wrong" in {
        val resource = simpleV(id, diskStorage, types = Set(nxv.Storage))
        Storage(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on S3Storage when types are wrong" in {
        val resource = simpleV(id, s3Storage, types = Set(nxv.Storage, nxv.S3Storage))
        Storage(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on DiskStorage when required parameters are not present" in {
        val resource = simpleV(id, diskStorage.removeKeys("volume"), types = Set(nxv.Storage, nxv.DiskStorage))
        Storage(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on S3Storage when required parameters are not present" in {
        val resource = simpleV(id, s3Storage.removeKeys("default"), types = Set(nxv.Storage, nxv.S3Storage, nxv.Alpha))
        Storage(resource).left.value shouldBe a[InvalidResourceFormat]
      }
    }

    "converting into json (from Graph)" should {
      "return the json representation for a query results list of DiskStorage" in {
        val diskStorage: DiskStorage =
          DiskStorage(projectRef, iri, 1L, false, false, "SHA-256", Paths.get("/tmp"), readDisk, writeDisk)
        val storages: QueryResults[Storage] =
          QueryResults(1L, List(UnscoredQueryResult(diskStorage)))
        StorageEncoder.json(storages).right.value should equalIgnoreArrayOrder(
          jsonContentOf("/storage/disk-storages.json"))
      }

      "return the json representation for a query results list of S3Storage" in {
        val settings  = S3Settings(Some(S3Credentials("access", "secret")), Some("endpoint"), Some("region"))
        val s3Storage = S3Storage(projectRef, iri, 1L, false, true, "MD5", "bucket", settings, readS3, writeS3)
        val storages: QueryResults[Storage] =
          QueryResults(1L, List(UnscoredQueryResult(s3Storage)))
        StorageEncoder.json(storages).right.value should equalIgnoreArrayOrder(
          jsonContentOf("/storage/s3-storages.json"))
      }
    }
  }

}
