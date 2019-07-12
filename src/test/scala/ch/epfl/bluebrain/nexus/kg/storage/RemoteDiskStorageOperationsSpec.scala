package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.file.Paths

import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.Uri
import akka.stream.{ActorMaterializer, Materializer}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.commons.test.io.IOEitherValues
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, Resources}
import ch.epfl.bluebrain.nexus.iam.client.types.{AuthToken, Permission}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes, FileDescription}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.RemoteDiskStorage
import ch.epfl.bluebrain.nexus.storage.client.StorageClient
import ch.epfl.bluebrain.nexus.storage.client.types.FileAttributes.{Digest => StorageDigest}
import ch.epfl.bluebrain.nexus.storage.client.types.{FileAttributes => StorageFileAttributes}
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest._

class RemoteDiskStorageOperationsSpec
    extends ActorSystemFixture("RemoteDiskStorageOperationsSpec")
    with WordSpecLike
    with Matchers
    with BeforeAndAfter
    with IdiomaticMockito
    with IOEitherValues
    with Resources
    with TestHelper {

  private val endpoint = "http://nexus.example.com/v1"

  private implicit val mt: Materializer = ActorMaterializer()

  private implicit val sc: StorageConfig = StorageConfig(
    DiskStorageConfig(Paths.get("/tmp"), "SHA-256", read, write, false, 1024L),
    RemoteDiskStorageConfig("http://example.com", None, "SHA-256", read, write, true, 1024L),
    S3StorageConfig("MD5", read, write, true, 1024L),
    "password",
    "salt"
  )

  sealed trait Ctx {
    val cred                              = genString()
    implicit val token: Option[AuthToken] = Some(AuthToken(cred))
    val path                              = Uri.Path(s"${genString()}/${genString()}")
    // format: off
    val storage = RemoteDiskStorage(ProjectRef(genUUID), genIri, 1L, false, false, "SHA-256", endpoint, Some(cred.encrypt), genString(), Permission.unsafe(genString()), Permission.unsafe(genString()), 1024L)
    val attributes = FileAttributes(s"$endpoint/${storage.folder}/$path", path, s"${genString()}.json", `application/json`, 12L, Digest("SHA-256", genString()))
    // format: on
  }

  private val client = mock[StorageClient[IO]]

  before {
    Mockito.reset(client)
  }

  private def consume(source: AkkaSource): String =
    source.runFold("")(_ ++ _.utf8String).futureValue

  "RemoteDiskStorageOperations" should {

    "verify when storage exists" in new Ctx {
      client.exists(storage.folder) shouldReturn IO(true)
      val verify = new RemoteDiskStorageOperations.Verify[IO](storage, client)
      verify.apply.accepted
    }

    "verify when storage does not exists" in new Ctx {
      client.exists(storage.folder) shouldReturn IO(false)
      val verify = new RemoteDiskStorageOperations.Verify[IO](storage, client)
      verify.apply
        .rejected[String] shouldEqual s"Folder '${storage.folder}' does not exists on the endpoint '${storage.endpoint}'"
    }

    "fetch file" in new Ctx {
      val source = genSource
      client.getFile(storage.folder, path) shouldReturn IO(source)
      val fetch        = new RemoteDiskStorageOperations.Fetch[IO](storage, client)
      val resultSource = fetch.apply(attributes).ioValue
      consume(resultSource) shouldEqual consume(source)
    }

    "link file" in new Ctx {
      val id               = Id(storage.ref, genIri)
      val sourcePath       = Uri.Path(s"${genString()}/${genString()}")
      val destRelativePath = Uri.Path(mangle(storage.ref, attributes.uuid, attributes.filename))
      client.moveFile(storage.folder, sourcePath, destRelativePath) shouldReturn
        IO(
          StorageFileAttributes(attributes.location,
                                attributes.bytes,
                                StorageDigest(attributes.digest.algorithm, attributes.digest.value)))
      val link = new RemoteDiskStorageOperations.Link[IO](storage, client)
      link
        .apply(id, FileDescription(attributes.uuid, attributes.filename, attributes.mediaType), sourcePath)
        .ioValue shouldEqual attributes.copy(path = destRelativePath)
    }
  }
}
