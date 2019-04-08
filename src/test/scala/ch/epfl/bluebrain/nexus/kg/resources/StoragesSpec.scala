package ch.epfl.bluebrain.nexus.kg.resources

import java.nio.file.Paths
import java.time.{Clock, Instant, ZoneId}

import akka.stream.ActorMaterializer
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, CirceEq, Randomness}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types.Permission
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.Verify
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{DiskStorage, S3Credentials, S3Settings, S3Storage, VerifyStorage}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Iri, RootedGraph}
import io.circe.Json
import org.mockito.IdiomaticMockito
import org.scalatest._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

//noinspection TypeAnnotation
class StoragesSpec
    extends ActorSystemFixture("StoragesSpec", true)
    with IOEitherValues
    with IOOptionValues
    with WordSpecLike
    with IdiomaticMockito
    with Matchers
    with OptionValues
    with EitherValues
    with Randomness
    with test.Resources
    with TestHelper
    with Inspectors
    with CirceEq {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3 second, 15 milliseconds)

  private implicit val appConfig              = Settings(system).appConfig
  private implicit val clock: Clock           = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

  private implicit val repo          = Repo[IO].ioValue
  private val storages: Storages[IO] = Storages[IO]
  private val readPerms              = Permission.unsafe("resources/read")
  private val writePerms             = Permission.unsafe("files/write")

  private val passVerify = new VerifyStorage[IO] {
    override def apply: IO[Either[String, Unit]] = IO.pure(Right(()))
  }

  trait Base {
    implicit val subject: Subject = Anonymous
    val projectRef                = ProjectRef(genUUID)
    val base                      = Iri.absolute(s"http://example.com/base/").right.value
    val id                        = Iri.absolute(s"http://example.com/$genUUID").right.value
    val resId                     = Id(projectRef, id)
    val voc                       = Iri.absolute(s"http://example.com/voc/").right.value
    // format: off
    implicit val project = Project(resId.value, "proj", "org", None, base, voc, Map.empty, projectRef.id, genUUID, 1L, deprecated = false, Instant.EPOCH, subject.id, Instant.EPOCH, subject.id)
    // format: on
    def updateId(json: Json) =
      json deepMerge Json.obj("@id" -> Json.fromString(id.show))

    val diskStorage = updateId(jsonContentOf("/storage/disk.json"))
    // format: off
    val diskStorageModel = DiskStorage(projectRef, id, 1L, deprecated = false, default = false, "SHA-256", Paths.get("/tmp"), readPerms, writePerms)
    // format: on

    implicit val verify = new Verify[IO] {
      override def apply(storage: Storage): VerifyStorage[IO] =
        if (storage == diskStorageModel || storage == s3StorageModelEncrypted || storage == diskStorageModel.copy(
              default = true)) passVerify
        else throw new RuntimeException
    }

    val typesDisk = Set[AbsoluteIri](nxv.Storage, nxv.DiskStorage)
    val s3Storage = updateId(jsonContentOf("/storage/s3.json"))
    // format: off
    val s3StorageModel = S3Storage(projectRef, id, 1L, deprecated = false, default = true, "SHA-256", "bucket", S3Settings(Some(S3Credentials("access", "secret")), Some("endpoint"), Some("region")), Permission.unsafe("my/read"), Permission.unsafe("my/write"))
    val s3StorageModelEncrypted = S3Storage(projectRef, id, 1L, deprecated = false, default = true, "SHA-256", "bucket", S3Settings(Some(S3Credentials("ByjwlDNy8D1Gm1o0EFCXwA==", "SjMIILT+A5BTUH4LP8sJBg==")), Some("endpoint"), Some("region")), Permission.unsafe("my/read"), Permission.unsafe("my/write"))
    // format: on
    val typesS3 = Set[AbsoluteIri](nxv.Storage, nxv.S3Storage)

    def resourceV(json: Json, rev: Long = 1L, types: Set[AbsoluteIri]): ResourceV = {
      val graph = (json deepMerge Json.obj("@id" -> Json.fromString(id.asString)))
        .replaceContext(storageCtx)
        .asGraph(resId.value)
        .right
        .value

      val resourceV =
        ResourceF.simpleV(resId, Value(json, storageCtx.contextValue, graph), rev, schema = storageRef, types = types)
      resourceV.copy(
        value = resourceV.value.copy(graph = RootedGraph(resId.value, graph.triples ++ resourceV.metadata())))
    }
  }

  "Storages bundle" when {

    "performing create operations" should {

      "prevent to create a storage that does not validate against the storage schema" in new Base {
        val invalid = List.range(1, 2).map(i => jsonContentOf(s"/storage/storage-wrong-$i.json"))
        forAll(invalid) { j =>
          val json = updateId(j)
          storages.create(base, json).value.rejected[InvalidResource]
        }
      }

      "create a DiskStorage" in new Base {
        val result = storages.create(base, diskStorage).value.accepted
        val expected =
          ResourceF.simpleF(resId, diskStorage, schema = storageRef, types = typesDisk)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "create a S3Storage" in new Base {
        val result   = storages.create(resId, s3Storage).value.accepted
        val expected = ResourceF.simpleF(resId, s3Storage, schema = storageRef, types = typesS3)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent creating a storage with the id passed on the call not matching the @id on the payload" in new Base {
        val json = diskStorage deepMerge Json.obj("@id" -> Json.fromString(genIri.asString))
        storages.create(resId, json).value.rejected[IncorrectId] shouldEqual IncorrectId(resId.ref)
      }

    }

    "performing update operations" should {

      "update a storage" in new Base {
        val storageUpdated = diskStorage deepMerge Json.obj("default" -> Json.fromBoolean(true))
        storages.create(resId, diskStorage).value.accepted shouldBe a[Resource]
        val result   = storages.update(resId, 1L, storageUpdated).value.accepted
        val expected = ResourceF.simpleF(resId, storageUpdated, 2L, schema = storageRef, types = typesDisk)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent to update a resolver that does not exists" in new Base {
        storages.update(resId, 1L, diskStorage).value.rejected[NotFound] shouldEqual
          NotFound(resId.ref, Some(1L))
      }
    }

    "performing deprecate operations" should {

      "deprecate a storage" in new Base {
        storages.create(resId, diskStorage).value.accepted shouldBe a[Resource]
        val result = storages.deprecate(resId, 1L).value.accepted
        val expected =
          ResourceF.simpleF(resId, diskStorage, 2L, schema = storageRef, types = typesDisk, deprecated = true)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent deprecating a resolver already deprecated" in new Base {
        storages.create(resId, diskStorage).value.accepted shouldBe a[Resource]
        storages.deprecate(resId, 1L).value.accepted shouldBe a[Resource]
        storages.deprecate(resId, 2L).value.rejected[ResourceIsDeprecated] shouldBe a[ResourceIsDeprecated]
      }
    }

    "performing read operations" should {
      val diskAddedJson = Json.obj("_algorithm" -> Json.fromString("SHA-256"),
                                   "writePermission" -> Json.fromString("files/write"),
                                   "readPermission"  -> Json.fromString("resources/read"))
      val s3AddedJson = Json.obj("_algorithm" -> Json.fromString("SHA-256"))

      "return a storage" in new Base {
        storages.create(resId, diskStorage).value.accepted shouldBe a[Resource]
        val result   = storages.fetch(resId).value.accepted
        val expected = resourceV(diskStorage deepMerge diskAddedJson, 1L, typesDisk)
        result.value.ctx shouldEqual expected.value.ctx
        result.value.graph shouldEqual expected.value.graph
        result shouldEqual expected.copy(value = result.value)
      }

      "return the requested storage on a specific revision" in new Base {
        storages.create(resId, diskStorage).value.accepted shouldBe a[Resource]
        storages.update(resId, 1L, s3Storage).value.accepted shouldBe a[Resource]

        val resultLatest = storages.fetch(resId, 2L).value.accepted
        val expectedLatest =
          resourceV(s3Storage.removeKeys("accessKey", "secretKey") deepMerge s3AddedJson, 2L, typesS3)
        resultLatest.value.ctx shouldEqual expectedLatest.value.ctx
        resultLatest.value.graph shouldEqual expectedLatest.value.graph
        resultLatest shouldEqual expectedLatest.copy(value = resultLatest.value)

        storages.fetch(resId, 2L).value.accepted shouldEqual storages.fetch(resId).value.accepted

        val result   = storages.fetch(resId, 1L).value.accepted
        val expected = resourceV(diskStorage deepMerge diskAddedJson, 1L, typesDisk)
        result.value.ctx shouldEqual expected.value.ctx
        result.value.graph shouldEqual expected.value.graph
        result shouldEqual expected.copy(value = result.value)
      }
    }
  }
}
