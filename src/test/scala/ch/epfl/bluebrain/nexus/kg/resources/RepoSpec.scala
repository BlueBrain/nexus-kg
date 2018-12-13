package ch.epfl.bluebrain.nexus.kg.resources

import java.nio.file.Paths
import java.time.{Clock, Instant, ZoneId}

import akka.stream.ActorMaterializer
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
import io.circe.Json
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.ExecutionContext

class RepoSpec
    extends ActorSystemFixture("RepoSpec", true)
    with WordSpecLike
    with IOEitherValues
    with IOOptionValues
    with Matchers
    with OptionValues
    with EitherValues
    with Randomness
    with MockitoSugar
    with BeforeAndAfter
    with TestHelper {

  private implicit val appConfig              = Settings(system).appConfig
  private implicit val clock: Clock           = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

  private val repo           = Repo[IO].ioValue
  private implicit val store = mock[AttachmentStore[IO, String, String]]

  private def randomJson() = Json.obj("key" -> Json.fromInt(genInt()))
  private def randomIri()  = Iri.absolute(s"http://example.com/$uuid").right.value

  before {
    Mockito.reset(store)
  }

  trait Context {
    val projectRef                  = ProjectRef(uuid)
    val iri                         = randomIri()
    val id                          = Id(projectRef, iri)
    val value                       = randomJson()
    val schema                      = Iri.absolute("http://example.com/schema").right.value
    implicit val identity: Identity = Anonymous
  }

  "A Repo" when {

    "performing create operations" should {
      "create a new resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldEqual
          ResourceF.simpleF(id, value, schema = Latest(schema))
      }

      "prevent to create a new resource that already exists" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.create(id, Latest(schema), Set.empty, value).value.rejected[AlreadyExists] shouldEqual AlreadyExists(
          id.ref)
      }
    }

    "performing update operations" should {
      "update a resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val types = Set(randomIri())
        private val json  = randomJson()
        repo.update(id, 1L, types, json).value.accepted shouldEqual
          ResourceF.simpleF(id, json, 2L, schema = Latest(schema), types = types)
      }

      "prevent to update a resource that does not exist" in new Context {
        repo.update(id, 1L, Set.empty, value).value.rejected[NotFound] shouldEqual NotFound(id.ref)
      }

      "prevent to update a resource with an incorrect revision" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val types = Set(randomIri())
        private val json  = randomJson()
        repo.update(id, 3L, types, json).value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 3L)
      }

      "prevent to update a deprecated resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        private val types = Set(randomIri())
        private val json  = randomJson()
        repo.update(id, 2L, types, json).value.rejected[IsDeprecated] shouldEqual IsDeprecated(id.ref)
      }

      "prevent to update a resource that was a Schema with a resource of another type" in new Context {
        repo.create(id, Latest(schema), Set(nxv.Schema), value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set(nxv.Ontology), json).value.rejected[UpdateSchemaTypes] shouldEqual UpdateSchemaTypes(
          id.ref)
      }

      "prevent to update a resource that was a Resolver with a resource of another type" in new Context {
        repo.create(id, Latest(schema), Set(nxv.Resolver), value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set(nxv.Schema), json).value.rejected[UpdateSchemaTypes] shouldEqual UpdateSchemaTypes(
          id.ref)
      }
    }

    "performing deprecate operations" should {
      "deprecate a resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]

        repo.deprecate(id, 1L).value.accepted shouldEqual
          ResourceF.simpleF(id, value, 2L, schema = Latest(schema), deprecated = true)
      }

      "prevent to deprecate a resource that does not exist" in new Context {
        repo.deprecate(id, 1L).value.rejected[NotFound] shouldEqual NotFound(id.ref)
      }

      "prevent to deprecate a resource with an incorrect revision" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 3L).value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 3L)
      }

      "prevent to deprecate a deprecated resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 2L).value.rejected[IsDeprecated] shouldEqual IsDeprecated(id.ref)
      }
    }

    "performing tag operations" should {
      "tag a resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set.empty, json).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 1L, "name").value.accepted shouldEqual
          ResourceF.simpleF(id, json, 3L, schema = Latest(schema)).copy(tags = Map("name" -> 1L))
      }

      "prevent to tag a resource that does not exist" in new Context {
        repo.tag(id, 1L, 1L, "name").value.rejected[NotFound] shouldEqual NotFound(id.ref)
      }

      "prevent to tag a resource with an incorrect revision" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.tag(id, 3L, 1L, "name").value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 3L)
      }

      "prevent to tag a deprecated resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 1L, "name").value.rejected[IsDeprecated] shouldEqual IsDeprecated(id.ref)
      }

      "prevent to tag a resource with a higher tag than current revision" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set.empty, json).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 4L, "name").value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 4L)
      }
    }

    "performing add attachment operations" should {

      "add several attachments to a resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val desc        = BinaryDescription("name", "text/plain")
        private val source      = "some text"
        private val desc2       = BinaryDescription("name2", "text/plain")
        private val source2     = "some text2"
        private val relative    = Paths.get("./other")
        private val attributes  = desc.process(StoredSummary(relative, 20L, Digest("MD5", "1234")))
        private val attributes2 = desc2.process(StoredSummary(relative, 30L, Digest("MD5", "4567")))
        when(store.save(id, desc, source)).thenReturn(EitherT.rightT[IO, Rejection](attributes))
        repo.attach(id, 1L, desc, source).value.accepted shouldEqual
          ResourceF.simpleF(id, value, 2L, schema = Latest(schema)).copy(attachments = Set(attributes))
        when(store.save(id, desc2, source2)).thenReturn(EitherT.rightT[IO, Rejection](attributes2))
        repo.attach(id, 2L, desc2, source2).value.accepted shouldEqual
          ResourceF.simpleF(id, value, 3L, schema = Latest(schema)).copy(attachments = Set(attributes, attributes2))
      }

      "prevent to add attachment to a resource that does not exist" in new Context {
        repo.attach(id, 1L, BinaryDescription("name", "text/plain"), "text").value.rejected[NotFound] shouldEqual
          NotFound(id.ref)
      }

      "prevent to add attachment to a resource with an incorrect revision" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.attach(id, 3L, BinaryDescription("name", "text/plain"), "text").value.rejected[IncorrectRev] shouldEqual
          IncorrectRev(id.ref, 3L)
      }

      "prevent to add attachment to a resource that is deprecated" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        repo.attach(id, 3L, BinaryDescription("name", "text/plain"), "text").value.rejected[IsDeprecated] shouldEqual
          IsDeprecated(id.ref)
      }

      "prevent to add attachment to a resource which fails on attempting to store" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val desc   = BinaryDescription("name", "text/plain")
        private val source = "some text"
        when(store.save(id, desc, source))
          .thenReturn(EitherT.leftT[IO, BinaryAttributes](Unexpected("error"): Rejection))
        repo.attach(id, 1L, desc, source).value.rejected[Unexpected] shouldEqual Unexpected("error")
      }
    }

    "performing remove attachments operations" should {
      "remove an attachment from a resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val desc       = BinaryDescription("name", "text/plain")
        private val source     = "some text"
        private val relative   = Paths.get("./other")
        private val attributes = desc.process(StoredSummary(relative, 20L, Digest("MD5", "1234")))
        when(store.save(id, desc, source)).thenReturn(EitherT.rightT[IO, Rejection](attributes))
        repo.attach(id, 1L, desc, source).value.accepted shouldBe a[Resource]
        repo.unattach(id, 2L, "name").value.accepted shouldEqual
          ResourceF.simpleF(id, value, 3L, schema = Latest(schema))
      }

      "prevent to remove attachment from a resource that does not exist" in new Context {
        repo.unattach(id, 2L, "name").value.rejected[NotFound] shouldEqual NotFound(id.ref)
      }

      "prevent to remove attachment from a resource with an incorrect revision" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.unattach(id, 3L, "name").value.rejected[IncorrectRev] shouldEqual IncorrectRev(id.ref, 3L)
      }

      "prevent to remove attachment from a deprecated resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.deprecate(id, 1L).value.accepted shouldBe a[Resource]
        repo.unattach(id, 2L, "name").value.rejected[IsDeprecated] shouldEqual IsDeprecated(id.ref)
      }

      "prevent to remove attachment from a resource without the provided attachment filename" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.unattach(id, 1L, "name").value.rejected[AttachmentNotFound] shouldEqual AttachmentNotFound(id.ref, "name")
      }
    }

    "performing get operations" should {
      "get a resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        repo.get(id).value.some shouldEqual ResourceF.simpleF(id, value, schema = Latest(schema))
      }

      "return None when the resource does not exist" in new Context {
        repo.get(id).value.ioValue shouldEqual None
      }

      "return a specific revision of the resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set(nxv.Resource), json).value.accepted shouldBe a[Resource]
        repo.get(id, 1L).value.some shouldEqual ResourceF.simpleF(id, value, 1L, schema = Latest(schema))
        repo.get(id, 2L).value.some shouldEqual
          ResourceF.simpleF(id, json, 2L, schema = Latest(schema), types = Set(nxv.Resource))
        repo.get(id, 2L).value.some shouldEqual repo.get(id).value.some
      }

      "return a specific tag of the resource" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        private val json = randomJson()
        repo.update(id, 1L, Set(nxv.Resource), json).value.accepted shouldBe a[Resource]
        repo.tag(id, 2L, 1L, "name").value.accepted shouldBe a[Resource]
        repo.tag(id, 3L, 2L, "other").value.accepted shouldBe a[Resource]

        repo.get(id, "name").value.some shouldEqual ResourceF.simpleF(id, value, 1L, schema = Latest(schema))
        repo.get(id, "other").value.some shouldEqual
          ResourceF.simpleF(id, json, 2L, schema = Latest(schema), types = Set(nxv.Resource))
      }
    }

    "performing get attachment operations" should {
      val relative           = Paths.get("./other")
      val desc               = BinaryDescription("name", "text/plain")
      val source             = "some text"
      val attributes         = desc.process(StoredSummary(relative, 20L, Digest("MD5", "1234")))
      val desc2              = BinaryDescription("name2", "text/plain")
      val source2            = "some text2"
      val attributes2        = desc2.process(StoredSummary(relative, 30L, Digest("MD5", "4567")))
      val source2Updated     = "some text2 other"
      val attributes2Updated = desc2.process(StoredSummary(relative, 40L, Digest("MD5", "910232")))

      "get a resource attachment" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        when(store.save(id, desc, source)).thenReturn(EitherT.rightT[IO, Rejection](attributes))
        repo.attach(id, 1L, desc, source).value.accepted shouldBe a[Resource]
        when(store.save(id, desc2, source2)).thenReturn(EitherT.rightT[IO, Rejection](attributes2))
        repo.attach(id, 2L, desc2, source2).value.accepted shouldBe a[Resource]
        when(store.save(id, desc2, source2Updated)).thenReturn(EitherT.rightT[IO, Rejection](attributes2Updated))
        repo.attach(id, 3L, desc2, source2Updated).value.accepted shouldBe a[Resource]
        when(store.fetch(attributes)).thenReturn(Right(source))

        repo.getAttachment(id, "name").value.some shouldEqual (attributes -> source)
      }

      "return None when the resource attachment does not exist" in new Context {
        repo.getAttachment(id, "name4").value.ioValue shouldEqual None
      }

      "return a specific revision of the resource attachment" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        when(store.save(id, desc, source)).thenReturn(EitherT.rightT[IO, Rejection](attributes))
        repo.attach(id, 1L, desc, source).value.accepted shouldBe a[Resource]
        when(store.save(id, desc2, source2)).thenReturn(EitherT.rightT[IO, Rejection](attributes2))
        repo.attach(id, 2L, desc2, source2).value.accepted shouldBe a[Resource]
        when(store.save(id, desc2, source2Updated)).thenReturn(EitherT.rightT[IO, Rejection](attributes2Updated))
        repo.attach(id, 3L, desc2, source2Updated).value.accepted shouldBe a[Resource]

        repo.getAttachment(id, 2L, "name2").value.ioValue shouldEqual None
        when(store.fetch(attributes2)).thenReturn(Right(source2))
        repo.getAttachment(id, 3L, "name2").value.some shouldEqual (attributes2 -> source2)
        when(store.fetch(attributes2Updated)).thenReturn(Right(source2Updated))
        repo.getAttachment(id, 4L, "name2").value.some shouldEqual (attributes2Updated -> source2Updated)
        repo.getAttachment(id, 4L, "name2").value.some shouldEqual repo.getAttachment(id, "name2").value.some
      }

      "return a specific tag of the resource attachment" in new Context {
        repo.create(id, Latest(schema), Set.empty, value).value.accepted shouldBe a[Resource]
        when(store.save(id, desc, source)).thenReturn(EitherT.rightT[IO, Rejection](attributes))
        repo.attach(id, 1L, desc, source).value.accepted shouldBe a[Resource]
        when(store.save(id, desc2, source2)).thenReturn(EitherT.rightT[IO, Rejection](attributes2))
        repo.attach(id, 2L, desc2, source2).value.accepted shouldBe a[Resource]
        when(store.save(id, desc2, source2Updated)).thenReturn(EitherT.rightT[IO, Rejection](attributes2Updated))
        repo.attach(id, 3L, desc2, source2Updated).value.accepted shouldBe a[Resource]
        repo.tag(id, 4L, 2L, "one").value.accepted shouldBe a[Resource]
        repo.tag(id, 5L, 3L, "two").value.accepted shouldBe a[Resource]
        repo.tag(id, 6L, 4L, "three").value.accepted shouldBe a[Resource]

        repo.getAttachment(id, "one", "name2").value.ioValue shouldEqual None
        when(store.fetch(attributes2)).thenReturn(Right(source2))
        repo.getAttachment(id, "two", "name2").value.some shouldEqual (attributes2 -> source2)
        when(store.fetch(attributes2Updated)).thenReturn(Right(source2Updated))
        repo.getAttachment(id, "three", "name2").value.some shouldEqual (attributes2Updated -> source2Updated)
        repo.getAttachment(id, "three", "name2").value.some shouldEqual repo.getAttachment(id, "name2").value.some
      }
    }
  }
}
