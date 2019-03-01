package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.Paths
import java.time.{Clock, Instant, ZoneId}

import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{FileStorage, S3Storage}
import ch.epfl.bluebrain.nexus.kg.storage.StorageEncoder._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import org.scalatest.{Inspectors, Matchers, OptionValues, WordSpecLike}

class StorageSpec extends WordSpecLike with Matchers with OptionValues with Resources with TestHelper with Inspectors {
  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  "A Storage" when {
    val iri        = url"http://example.com/id".value
    val iri2       = url"http://example.com/id2".value
    val projectRef = ProjectRef(genUUID)
    val id         = Id(projectRef, iri)

    "constructing" should {
      val fileStorage = jsonContentOf("/storage/file.json").appendContextOf(storageCtx)
      val s3Storage   = jsonContentOf("/storage/s3.json").appendContextOf(storageCtx)

      "return a FileStorage" in {
        val resource = simpleV(id, fileStorage, types = Set(nxv.Storage, nxv.FileStorage))
        Storage(resource).right.value shouldEqual
          FileStorage(projectRef, iri, 1L, clock.instant(), false, false, "SHA-256", Paths.get("/tmp"))
      }

      "return an S3Storage" in {
        val resource = simpleV(id, s3Storage, types = Set(nxv.Storage, nxv.S3Storage, nxv.Alpha))
        Storage(resource).right.value shouldEqual
          S3Storage(projectRef, iri, 1L, clock.instant(), false, true, "MD-5")
      }

      "fail on FileStorage when types are wrong" in {
        val resource = simpleV(id, fileStorage, types = Set(nxv.Storage))
        Storage(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on S3Storage when types are wrong" in {
        val resource = simpleV(id, s3Storage, types = Set(nxv.Storage, nxv.S3Storage))
        Storage(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on FileStorage when required parameters are not present" in {
        val resource = simpleV(id, fileStorage.removeKeys("volume"), types = Set(nxv.Storage, nxv.FileStorage))
        Storage(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on S3Storage when required parameters are not present" in {
        val resource = simpleV(id, s3Storage.removeKeys("default"), types = Set(nxv.Storage, nxv.S3Storage, nxv.Alpha))
        Storage(resource).left.value shouldBe a[InvalidResourceFormat]
      }
    }

    "converting into json (from Graph)" should {
      "return the json representation for a query results list" in {
        val fileStorage: FileStorage =
          FileStorage(projectRef, iri, 1L, clock.instant(), false, false, "SHA-256", Paths.get("/tmp"))
        val s3Storage = S3Storage(projectRef, iri2, 1L, clock.instant(), false, true, "MD-5")
        val storages: QueryResults[Storage] =
          QueryResults(2L, List(UnscoredQueryResult(fileStorage), UnscoredQueryResult(s3Storage)))
        StorageEncoder.json(storages).right.value should equalIgnoreArrayOrder(jsonContentOf("/storage/storages.json"))
      }
    }
  }

}
