package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.Paths

import akka.http.scaladsl.model.{ContentTypes, Uri}
import akka.stream.ActorMaterializer
import cats.effect.IO
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.test.io.IOEitherValues
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileDescription
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.{KgError, TestHelper}
import org.mockito.IdiomaticMockito
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

class DiskStorageOperationsSpec
    extends ActorSystemFixture("DiskStorageOperationsSpec")
    with WordSpecLike
    with Matchers
    with BeforeAndAfter
    with IdiomaticMockito
    with IOEitherValues
    with Resources
    with TestHelper {

  private implicit val sc: StorageConfig = StorageConfig(
    DiskStorageConfig(Paths.get("/tmp"), "SHA-256", read, write, false, 1024L),
    RemoteDiskStorageConfig("http://example.com", None, "SHA-256", read, write, true, 1024L),
    S3StorageConfig("MD5", read, write, true, 1024L),
    "password",
    "salt"
  )

  private val project  = ProjectRef(genUUID)
  private val storage  = Storage.DiskStorage.default(project)
  private val resId    = Id(storage.ref, genIri)
  private val fileDesc = FileDescription("my file.txt", ContentTypes.`text/plain(UTF-8)`)

  private def consume(source: AkkaSource): String =
    source.runFold("")(_ ++ _.utf8String)(ActorMaterializer()).futureValue

  "RemoteDiskStorageOperations" should {

    "verify when the storage exists" in {
      val verify = new DiskStorageOperations.VerifyDiskStorage[IO](storage)
      verify.apply.accepted
    }

    "save and fetch files" in {
      val save   = new DiskStorageOperations.SaveDiskFile[IO](storage)
      val fetch  = new DiskStorageOperations.FetchDiskFile[IO]()
      val source = genSource

      val attr = save.apply(resId, fileDesc, source).ioValue
      attr.bytes shouldEqual 16L
      attr.filename shouldEqual fileDesc.filename
      attr.mediaType shouldEqual fileDesc.mediaType
      attr.location shouldEqual Uri(s"file:///tmp/${mangle(project, attr.uuid, "my%20file.txt")}")
      attr.path shouldEqual attr.location.path.tail.tail.tail
      val fetched = fetch.apply(attr).ioValue

      consume(source) shouldEqual consume(fetched)
    }

    "not link files" in {
      val link = new DiskStorageOperations.LinkDiskFile[IO]()
      link.apply(resId, fileDesc, Uri.Path("/foo")).failed[KgError] shouldEqual KgError.UnsupportedOperation
    }
  }

}
