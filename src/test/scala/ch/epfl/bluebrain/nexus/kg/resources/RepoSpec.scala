package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import cats.effect.{ContextShift, IO, Timer}
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, Randomness}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, Subject}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.{Link, Save}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{DiskStorage, LinkFile, SaveFile}
import ch.epfl.bluebrain.nexus.kg.{KgError, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri
import io.circe.Json
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

//noinspection RedundantDefaultArgument
class RepoSpec
    extends ActorSystemFixture("RepoSpec", true)
    with WordSpecLike
    with IOEitherValues
    with IOOptionValues
    with Matchers
    with OptionValues
    with EitherValues
    with Randomness
    with IdiomaticMockito
    with BeforeAndAfter
    with TestHelper {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3 second, 15 milliseconds)

  private implicit val appConfig: AppConfig   = Settings(system).appConfig
  private implicit val clock: Clock           = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

  private val repo     = Repo[IO].ioValue
  private val saveFile = mock[SaveFile[IO, String]]
  private val linkFile = mock[LinkFile[IO]]

  private def randomJson() = Json.obj("key" -> Json.fromInt(genInt()))
  private def randomIri()  = Iri.absolute(s"http://example.com/$genUUID").right.value

  before {
    Mockito.reset(saveFile)
  }

  //noinspection TypeAnnotation
  trait Context {
    val projectRef                = ProjectRef(genUUID)
    val organizationRef           = OrganizationRef(genUUID)
    val iri                       = randomIri()
    val id                        = Id(projectRef, iri)
    val value                     = randomJson()
    val schema                    = Iri.absolute("http://example.com/schema").right.value
    implicit val subject: Subject = Anonymous
  }

  //noinspection TypeAnnotation
  trait File extends Context {
    override val value  = Json.obj()
    override val schema = fileSchemaUri
    val types           = Set(nxv.File.value)
    val storage         = DiskStorage.default(projectRef)

    implicit val save: Save[IO, String] = (st: Storage) => if (st == storage) saveFile else throw new RuntimeException
    implicit val link: Link[IO]         = (st: Storage) => if (st == storage) linkFile else throw new RuntimeException
  }

  "A Repo" when {

    "performing create operations" should {
      "create a new resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldEqual
          ResourceF.simpleF(id, value, schema = Latest(schema))
      }

      "prevent to create a new resource that already exists" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo
          .create(id, organizationRef, Latest(schema), Set.empty, value)
          .value
          .rejected[ResourceAlreadyExists] shouldEqual ResourceAlreadyExists(id.ref)
      }
    }

    "performing update operations" should {
      "update a resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val types = Set(randomIri())
        private val json  = randomJson()
        repo.update(id, 1L, types, json).value.accepted shouldEqual
          ResourceF.simpleF(id, json, 2L, schema = Latest(schema), types = types)
      }

      "prevent to update a resource that does not exist" in new Context {
        repo.update(id, 1L, Set.empty, value).value.rejected[NotFound] shouldEqual NotFound(id.ref)
      }

      "prevent to update a resource with an incorrect revision" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val types = Set(randomIri())
        private val json  = randomJson()
        repo.update(id, 3L, types, json).value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 3L, 1L)
      }

      "prevent to update a deprecated resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        private val types = Set(randomIri())
        private val json  = randomJson()
        repo.update(id, 2L, types, json).value.rejected[ResourceIsDeprecated] shouldEqual ResourceIsDeprecated(id.ref)
      }
    }

    "performing deprecate operations" should {
      "deprecate a resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]

        repo.deprecate(id, 1L).value.accepted shouldEqual
          ResourceF.simpleF(id, value, 2L, schema = Latest(schema), deprecated = true)
      }

      "prevent to deprecate a resource that does not exist" in new Context {
        repo.deprecate(id, 1L).value.rejected[NotFound] shouldEqual NotFound(id.ref)
      }

      "prevent to deprecate a resource with an incorrect revision" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 3L).value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 3L, 1L)
      }

      "prevent to deprecate a deprecated resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 2L).value.rejected[ResourceIsDeprecated] shouldEqual ResourceIsDeprecated(id.ref)
      }
    }

    "performing tag operations" should {
      "tag a resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set.empty, json).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 1L, "name").value.accepted shouldEqual
          ResourceF.simpleF(id, json, 3L, schema = Latest(schema)).copy(tags = Map("name" -> 1L))
      }

      "prevent to tag a resource that does not exist" in new Context {
        repo.tag(id, 1L, 1L, "name").value.rejected[NotFound] shouldEqual NotFound(id.ref)
      }

      "prevent to tag a resource with an incorrect revision" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.tag(id, 3L, 1L, "name").value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 3L, 1L)
      }

      "prevent to tag a deprecated resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 1L, "name").value.rejected[ResourceIsDeprecated] shouldEqual ResourceIsDeprecated(id.ref)
      }

      "prevent to tag a resource with a higher tag than current revision" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set.empty, json).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 4L, "name").value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 4L, 2L)
      }
    }

    "performing file operations" should {
      val desc        = FileDescription("name", `text/plain(UTF-8)`)
      val source      = "some text"
      val desc2       = FileDescription("name2", `text/plain(UTF-8)`)
      val source2     = "some text2"
      val location    = Uri("file:///tmp/other")
      val path        = Uri.Path("other")
      val attributes  = desc.process(StoredSummary(location, path, 20L, Digest("MD5", "1234")))
      val attributes2 = desc2.process(StoredSummary(location, path, 30L, Digest("MD5", "4567")))

      "create file resource" in new File {
        saveFile(id, desc, source) shouldReturn IO.pure(attributes)

        repo.createFile(id, organizationRef, storage, desc, source).value.accepted shouldEqual
          ResourceF.simpleF(id, value, 1L, types, schema = Latest(schema)).copy(file = Some(storage -> attributes))
      }

      "update the file resource" in new File {
        saveFile(id, desc, source) shouldReturn IO.pure(attributes)
        saveFile(id, desc, source2) shouldReturn IO.pure(attributes2)

        repo.createFile(id, organizationRef, storage, desc, source).value.accepted shouldBe a[Resource]
        repo.updateFile(id, storage, 1L, desc, source2).value.accepted shouldEqual
          ResourceF.simpleF(id, value, 2L, types, schema = Latest(schema)).copy(file = Some(storage -> attributes2))
      }

      "prevent to update a file resource with an incorrect revision" in new File {
        saveFile(id, desc, source) shouldReturn IO.pure(attributes)

        repo.createFile(id, organizationRef, storage, desc, source).value.accepted shouldBe a[Resource]
        repo.updateFile(id, storage, 3L, desc, source).value.rejected[IncorrectRev] shouldEqual
          IncorrectRev(id.ref, 3L, 1L)
      }

      "prevent update a file resource to a deprecated resource" in new File {
        saveFile(id, desc, source) shouldReturn IO.pure(attributes)
        repo.createFile(id, organizationRef, storage, desc, source).value.accepted shouldBe a[Resource]

        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        repo
          .updateFile(id, storage, 2L, desc, source)
          .value
          .rejected[ResourceIsDeprecated] shouldEqual ResourceIsDeprecated(id.ref)
      }

      "prevent to create a file resource which fails on attempting to store" in new File {
        saveFile(id, desc, source) shouldReturn IO.raiseError(KgError.InternalError(""))
        repo.createFile(id, organizationRef, storage, desc, source).value.failed[KgError.InternalError]
      }
    }

    "performing link operations" should {
      val desc        = FileDescription("name", `text/plain(UTF-8)`)
      val desc2       = FileDescription("name2", `text/plain(UTF-8)`)
      val location    = Uri("file:///tmp/other")
      val path        = Uri.Path("other")
      val location2   = Uri("file:///tmp/other2")
      val path2       = Uri.Path("other2")
      val attributes  = desc.process(StoredSummary(location, path, 20L, Digest("MD5", "1234")))
      val attributes2 = desc2.process(StoredSummary(location2, path2, 30L, Digest("MD5", "4567")))

      "create link" in new File {
        linkFile(id, desc, path) shouldReturn IO.pure(attributes)

        repo.createLink(id, organizationRef, storage, desc, path).value.accepted shouldEqual
          ResourceF.simpleF(id, value, 1L, types, schema = Latest(schema)).copy(file = Some(storage -> attributes))
      }

      "update link" in new File {
        linkFile(id, desc, path) shouldReturn IO.pure(attributes)
        linkFile(id, desc, path2) shouldReturn IO.pure(attributes2)

        repo.createLink(id, organizationRef, storage, desc, path).value.accepted shouldBe a[Resource]
        repo.updateLink(id, storage, desc, path2, 1L).value.accepted shouldEqual
          ResourceF.simpleF(id, value, 2L, types, schema = Latest(schema)).copy(file = Some(storage -> attributes2))
      }

      "prevent link update with an incorrect revision" in new File {
        linkFile(id, desc, path) shouldReturn IO.pure(attributes)

        repo.createLink(id, organizationRef, storage, desc, path).value.accepted shouldBe a[Resource]
        repo.updateLink(id, storage, desc, path, 3L).value.rejected[IncorrectRev] shouldEqual
          IncorrectRev(id.ref, 3L, 1L)
      }

      "prevent link update to a deprecated resource" in new File {
        linkFile(id, desc, path) shouldReturn IO.pure(attributes)
        repo.createLink(id, organizationRef, storage, desc, path).value.accepted shouldBe a[Resource]

        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        repo
          .updateLink(id, storage, desc, path, 2L)
          .value
          .rejected[ResourceIsDeprecated] shouldEqual ResourceIsDeprecated(id.ref)
      }

      "prevent link creation when the store operation fails" in new File {
        linkFile(id, desc, path) shouldReturn IO.raiseError(KgError.InternalError(""))
        repo.createLink(id, organizationRef, storage, desc, path).value.failed[KgError.InternalError]
      }
    }

    "performing get operations" should {
      "get a resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.get(id, None).value.some shouldEqual ResourceF.simpleF(id, value, schema = Latest(schema))
      }

      "return None when the resource does not exist" in new Context {
        repo.get(id, None).value.ioValue shouldEqual None
      }

      "return None when getting a resource from the wrong schema" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.get(id, Some(genIri.ref)).value.ioValue shouldEqual None
        repo.get(id, 1L, Some(genIri.ref)).value.ioValue shouldEqual None
      }

      "return a specific revision of the resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set(nxv.Resource), json).value.accepted shouldBe a[Resource]
        repo.get(id, 1L, None).value.some shouldEqual
          ResourceF.simpleF(id, value, 1L, schema = Latest(schema))
        repo.get(id, 2L, None).value.some shouldEqual
          ResourceF.simpleF(id, json, 2L, schema = Latest(schema), types = Set(nxv.Resource))
        repo.get(id, 2L, None).value.some shouldEqual repo.get(id, None).value.some
      }

      "return a specific tag of the resource" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set(nxv.Resource), json).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 1L, "name").value.accepted shouldBe a[Resource]
        repo.tag(id, 3L, 2L, "other").value.accepted shouldBe a[Resource]

        repo.get(id, "name", None).value.some shouldEqual ResourceF.simpleF(id, value, 1L, schema = Latest(schema))
        repo.get(id, "other", None).value.some shouldEqual
          ResourceF.simpleF(id, json, 2L, schema = Latest(schema), types = Set(nxv.Resource))
      }
    }

    "performing get tag operations" should {
      "get a resource tag" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set.empty, json).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 1L, "name").value.accepted shouldEqual
          ResourceF.simpleF(id, json, 3L, schema = Latest(schema)).copy(tags = Map("name" -> 1L))
        repo.get(id, None).value.some.tags shouldEqual Map("name" -> 1L)
      }

      "return None when the resource does not exist" in new Context {
        repo.get(id, None).value.ioValue shouldEqual None
      }

      "return None when getting a resource from the wrong schema" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.get(id, Some(genIri.ref)).value.ioValue shouldEqual None
        repo.get(id, 1L, Some(genIri.ref)).value.ioValue shouldEqual None
      }

      "return a specific revision of the resource tags" in new Context {
        repo.create(id, organizationRef, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set.empty, json).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 1L, "name").value.accepted shouldEqual
          ResourceF.simpleF(id, json, 3L, schema = Latest(schema)).copy(tags = Map("name" -> 1L))
        repo.tag(id, 3L, 1L, "name2").value.accepted shouldEqual
          ResourceF.simpleF(id, json, 4L, schema = Latest(schema)).copy(tags = Map("name" -> 1L, "name2" -> 1L))

        repo.get(id, None).value.some.tags shouldEqual Map("name"     -> 1L, "name2" -> 1L)
        repo.get(id, 4L, None).value.some.tags shouldEqual Map("name" -> 1L, "name2" -> 1L)
        repo.get(id, 3L, None).value.some.tags shouldEqual Map("name" -> 1L)
        repo.get(id, "name", None).value.some.tags shouldEqual Map()
      }
    }

    "performing get file operations" should {
      val location    = Uri("file:///tmp/other")
      val path        = Uri.Path("other")
      val desc        = FileDescription("name", `text/plain(UTF-8)`)
      val source      = "some text"
      val attributes  = desc.process(StoredSummary(location, path, 20L, Digest("MD5", "1234")))
      val desc2       = FileDescription("name2", `text/plain(UTF-8)`)
      val source2     = "some text2"
      val attributes2 = desc2.process(StoredSummary(location, path, 30L, Digest("MD5", "4567")))

      "get a file resource" in new File {
        saveFile(id, desc, source) shouldReturn IO.pure(attributes)
        repo.createFile(id, organizationRef, storage, desc, source).value.accepted shouldBe a[Resource]

        saveFile(id, desc2, source2) shouldReturn IO.pure(attributes2)
        repo.updateFile(id, storage, 1L, desc2, source2).value.accepted shouldBe a[Resource]

        repo.get(id, None).value.some.file.value shouldEqual (storage -> attributes2)

        //by rev
        repo.get(id, 2L, None).value.some.file.value shouldEqual (storage -> attributes2)

        repo.get(id, 1L, None).value.some.file.value shouldEqual (storage -> attributes)

        //by tag
        repo.tag(id, 2L, 1L, "one").value.accepted shouldBe a[Resource]
        repo.tag(id, 3L, 2L, "two").value.accepted shouldBe a[Resource]
        repo.get(id, "one", None).value.some.file.value shouldEqual (storage -> attributes)
        repo.get(id, "two", None).value.some.file.value shouldEqual (storage -> attributes2)

      }

      "return None when the file resource does not exist" in new File {
        repo.get(id, "name4", None).value.ioValue shouldEqual None
        repo.get(id, 2L, None).value.ioValue shouldEqual None
        repo.get(id, None).value.ioValue shouldEqual None
      }
    }
  }
}
