package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.Paths
import java.time.{Clock, Instant, ZoneId}

import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{DiskStorageConfig, S3StorageConfig, StorageConfig}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{DiskStorage, S3Storage}
import ch.epfl.bluebrain.nexus.kg.storage.StorageEncoder._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import org.scalatest.{Inspectors, Matchers, OptionValues, WordSpecLike}

class StorageSpec extends WordSpecLike with Matchers with OptionValues with Resources with TestHelper with Inspectors {
  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val storageConfig =
    StorageConfig(DiskStorageConfig(Paths.get("/tmp/"), "SHA-256"), S3StorageConfig("MD-5"))
  "A Storage" when {
    val iri        = url"http://example.com/id".value
    val projectRef = ProjectRef(genUUID)
    val id         = Id(projectRef, iri)

    "constructing" should {
      val diskStorage = jsonContentOf("/storage/disk.json").appendContextOf(storageCtx)
      val s3Storage   = jsonContentOf("/storage/s3.json").appendContextOf(storageCtx)

      "return a DiskStorage" in {
        val resource = simpleV(id, diskStorage, types = Set(nxv.Storage, nxv.DiskStorage))
        Storage(resource).right.value shouldEqual
          DiskStorage(projectRef, iri, 1L, false, false, "SHA-256", Paths.get("/tmp"))
      }

      "return an S3Storage" in {
        val resource = simpleV(id, s3Storage, types = Set(nxv.Storage, nxv.S3Storage, nxv.Alpha))
        Storage(resource).right.value shouldEqual
          S3Storage(projectRef, iri, 1L, false, true, "MD-5")
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
          DiskStorage(projectRef, iri, 1L, false, false, "SHA-256", Paths.get("/tmp"))
        val storages: QueryResults[Storage] =
          QueryResults(1L, List(UnscoredQueryResult(diskStorage)))
        StorageEncoder.json(storages).right.value should equalIgnoreArrayOrder(
          jsonContentOf("/storage/disk-storages.json"))
      }

      "return the json representation for a query results list of S3Storage" in {
        val s3Storage = S3Storage(projectRef, iri, 1L, false, true, "MD-5")
        val storages: QueryResults[Storage] =
          QueryResults(1L, List(UnscoredQueryResult(s3Storage)))
        StorageEncoder.json(storages).right.value should equalIgnoreArrayOrder(
          jsonContentOf("/storage/s3-storages.json"))
      }
    }
  }

}
