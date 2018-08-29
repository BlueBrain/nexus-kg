package ch.epfl.bluebrain.nexus.kg.indexing

import java.nio.file.Paths
import java.time.{Clock, Instant}

import akka.actor.ActorSystem
import cats.data.{EitherT, OptionT}
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.Schemas.resourceSchemaUri
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.BinaryAttributes
import ch.epfl.bluebrain.nexus.kg.resources.v0.Event
import ch.epfl.bluebrain.nexus.rdf.Iri
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.ArgumentCaptor
import org.mockito.Mockito
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar

class MigrationIndexerSpec
    extends WordSpecLike
    with MockitoSugar
    with Matchers
    with Resources
    with BeforeAndAfter
    with EitherValues
    with ScalaFutures
    with Inspectors {

  private val as             = ActorSystem("MigrationIndexerSpec")
  private val repo           = mock[Repo[Task]]
  private val resourceSchema = Ref(resourceSchemaUri)
  private val projectRef     = ProjectRef("org/v0")
  private val base           = Iri.absolute("https://nexus.example.com").right.value
  private val resource       = ResourceF.simpleF(Id(projectRef, base + "some-id"), Json.obj())(Clock.systemUTC)
  private val success        = EitherT.right[Rejection](Task(resource))
  private val indexer        = new MigrationIndexer(repo, "topic", base, projectRef)(as)

  before {
    Mockito.reset(repo)
  }

  "A MigrationIndexer" should {
    "process 'instance created' events" in {
      val (id, schema, types, source, instant, author) =
        (idCaptor, schemaCaptor, typesCaptor, sourceCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.create(id.capture, schema.capture, types.capture, source.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/instance-created.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "some/stuff/v0.1.1/2e838302-81df-48be-a348-ef45cbdb5ad0")
      schema.getValue shouldEqual resourceSchema
      types.getValue shouldEqual Set(nxv.Resource.value)
      source.getValue.asObject.getOrElse(fail).keys should contain("geneName")
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      instant.getValue shouldEqual Instant.parse("2018-07-16T07:04:08.799Z")
    }

    "process 'context created'" in {
      val (id, schema, types, source, instant, author) =
        (idCaptor, schemaCaptor, typesCaptor, sourceCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.create(id.capture, schema.capture, types.capture, source.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/context-created.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "neurosciencegraph/core/data/v1.0.3")
      schema.getValue shouldEqual resourceSchema
      types.getValue shouldEqual Set(nxv.Resource.value)
      source.getValue.asObject.getOrElse(fail).keys.toList shouldEqual List("@context")
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-456789abcdef:alice")
      instant.getValue shouldEqual Instant.parse("2018-07-30T15:09:16.017Z")
    }

    "process 'schema created'" in {
      val (id, schema, types, source, instant, author) =
        (idCaptor, schemaCaptor, typesCaptor, sourceCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.create(id.capture, schema.capture, types.capture, source.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/schema-created.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "some/sim/tool/v0.1.2")
      schema.getValue shouldEqual resourceSchema
      types.getValue shouldEqual Set(nxv.Resource.value, nxv.Schema.value)
      source.getValue.asObject.getOrElse(fail).keys should contain("shapes")
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      instant.getValue shouldEqual Instant.parse("2018-08-22T14:58:28.838Z")
    }

    "process 'instance attachment created' events" in {
      val (id, rev, attr, instant, author) = (idCaptor, revCaptor, attrCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.unsafeAttach(id.capture, rev.capture, attr.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/attachment-created.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef,
                                 base + "some/project/sim/script/v0.1.0/14572e6b-3811-4b48-9673-194059d40981")
      rev.getValue shouldEqual 2L
      val BinaryAttributes(_, filePath, filename, mediaType, contentSize, digest) = attr.getValue
      filePath.toString shouldEqual "1/4/5/7/2/e/6/b/14572e6b-3811-4b48-9673-194059d40981.1"
      filename shouldEqual "cACint_L23MC.hoc"
      mediaType shouldEqual "application/neuron-hoc"
      contentSize shouldEqual Attachment.Size("byte", 11641L)
      digest shouldEqual Attachment.Digest("SHA-256",
                                           "37d393c115f239bdc1782d494125dcd3e6a6aa2766ed0e28e837780830563ce2")
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-456789abcdef:alice")
      instant.getValue shouldEqual Instant.parse("2018-08-16T15:11:59.924Z")
    }

    "process 'instance attachment removed' events" in {
      val instanceId = Id(projectRef, base + "some/project/sim/script/v0.1.0/14572e6b-3811-4b48-9673-194059d40981")
      val (id, id2, rev, fileName, instant, author) =
        (idCaptor, idCaptor, revCaptor, stringCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.get(id.capture))
        .thenReturn(OptionT[Task, Resource](Task(Some(resource.copy(
          id = instanceId,
          attachments = Set(BinaryAttributes(Paths.get("null"),
                                             "fileName",
                                             "mediaType",
                                             Attachment.Size("bytes", 123L),
                                             Attachment.Digest("algo", "value")))
        )))))
      Mockito
        .when(repo.unattach(id2.capture, rev.capture, fileName.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/attachment-removed.json").as[Event].right.value
      indexer.process(event).value.runAsync.futureValue shouldEqual Right(resource)
      id.getValue shouldEqual instanceId
      id2.getValue shouldEqual instanceId
      rev.getValue shouldEqual 2L
      fileName.getValue shouldEqual "fileName"
      instant.getValue shouldEqual Instant.parse("2018-08-16T15:11:59.924Z")
    }

    "process 'instance updated' events" in {
      val (id, rev, types, source, instant, author) =
        (idCaptor, revCaptor, typesCaptor, sourceCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.update(id.capture, rev.capture, types.capture, source.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/instance-updated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "some/trace/v1.0.0/a5b09604-1301-452c-a1a4-4a580d4b24db")
      rev.getValue shouldEqual 5L
      types.getValue shouldEqual Set(nxv.Resource.value)
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-456789abcdef:alice")
      instant.getValue shouldEqual Instant.parse("2018-08-23T10:24:49.893Z")
    }

    "process 'context updated' events" in {
      val (id, rev, types, source, instant, author) =
        (idCaptor, revCaptor, typesCaptor, sourceCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.update(id.capture, rev.capture, types.capture, source.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/context-updated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "demo/core/data/v0.2.8")
      rev.getValue shouldEqual 2L
      types.getValue shouldEqual Set(nxv.Resource.value)
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      instant.getValue shouldEqual Instant.parse("2018-05-09T06:15:51.888Z")
    }

    "process 'schema updated' events" in {
      val (id, rev, types, source, instant, author) =
        (idCaptor, revCaptor, typesCaptor, sourceCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.update(id.capture, rev.capture, types.capture, source.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/schema-updated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "some/sim/tool/v0.1.2")
      rev.getValue shouldEqual 2L
      types.getValue shouldEqual Set(nxv.Resource.value, nxv.Schema.value)
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      instant.getValue shouldEqual Instant.parse("2018-08-22T14:58:28.838Z")
    }

    "process 'instance deprecated' events" in {
      val (id, rev, instant, author) = (idCaptor, revCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.deprecate(id.capture, rev.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/instance-deprecated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "some/other/stuff/v0.1.0/85b8bc79-8e25-4ae5-81fd-2e76559f6a60")
      rev.getValue shouldEqual 2L
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      instant.getValue shouldEqual Instant.parse("2018-08-27T13:16:35.621Z")
    }

    "process 'context deprecated' events" in {
      val (id, rev, instant, author) = (idCaptor, revCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.deprecate(id.capture, rev.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/context-deprecated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "neurosciencegraph/core/data/v1.0.3")
      rev.getValue shouldEqual 2L
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      instant.getValue shouldEqual Instant.parse("2018-08-27T13:16:35.621Z")
    }

    "process 'schema deprecated' events" in {
      val (id, rev, instant, author) = (idCaptor, revCaptor, instantCaptor, authorCaptor)
      Mockito
        .when(repo.deprecate(id.capture, rev.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/schema-deprecated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
      id.getValue shouldEqual Id(projectRef, base + "some/sim/tool/v0.1.2")
      rev.getValue shouldEqual 3L
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      instant.getValue shouldEqual Instant.parse("2018-08-24T11:29:21.316Z")
    }

    "process 'context published' events" in {
      val contextId = Id(projectRef, base + "nexus/core/resource/v0.1.0")
      val (id, id2, rev, types, source, instant, author) =
        (idCaptor, idCaptor, revCaptor, typesCaptor, sourceCaptor, instantCaptor, authorCaptor)
      Mockito.when(repo.get(id.capture)).thenReturn(OptionT[Task, Resource](Task(Some(resource.copy(id = contextId)))))
      Mockito
        .when(repo.update(id2.capture, rev.capture, types.capture, source.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/context-published.json").as[Event].right.value
      indexer.process(event).value.runAsync.futureValue shouldEqual Right(resource)
      id.getValue shouldEqual contextId
      id2.getValue shouldEqual contextId
      rev.getValue shouldEqual 2L
      author.getValue shouldEqual Anonymous
      instant.getValue shouldEqual Instant.parse("2018-01-18T23:52:49.563Z")
    }

    "process 'schema published' events" in {
      val schemaId = Id(projectRef, base + "some/sim/tool/v0.1.2")
      val (id, id2, rev, types, source, instant, author) =
        (idCaptor, idCaptor, revCaptor, typesCaptor, sourceCaptor, instantCaptor, authorCaptor)
      Mockito.when(repo.get(id.capture)).thenReturn(OptionT[Task, Resource](Task(Some(resource.copy(id = schemaId)))))
      Mockito
        .when(repo.update(id2.capture, rev.capture, types.capture, source.capture, instant.capture)(author.capture))
        .thenReturn(success)
      val event = jsonContentOf("/v0/schema-published.json").as[Event].right.value
      indexer.process(event).value.runAsync.futureValue shouldEqual Right(resource)
      id.getValue shouldEqual schemaId
      id2.getValue shouldEqual schemaId
      rev.getValue shouldEqual 2L
      author.getValue shouldEqual UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      instant.getValue shouldEqual Instant.parse("2018-08-22T15:00:43.408Z")
    }
  }

  private def idCaptor: ArgumentCaptor[Id[ProjectRef]]          = ArgumentCaptor.forClass(classOf[Id[ProjectRef]])
  private def revCaptor: ArgumentCaptor[Long]                   = ArgumentCaptor.forClass(classOf[Long])
  private def stringCaptor: ArgumentCaptor[String]              = ArgumentCaptor.forClass(classOf[String])
  private def schemaCaptor: ArgumentCaptor[Ref]                 = ArgumentCaptor.forClass(classOf[Ref])
  private def typesCaptor: ArgumentCaptor[Set[Iri.AbsoluteIri]] = ArgumentCaptor.forClass(classOf[Set[Iri.AbsoluteIri]])
  private def sourceCaptor: ArgumentCaptor[Json]                = ArgumentCaptor.forClass(classOf[Json])
  private def authorCaptor: ArgumentCaptor[Identity]            = ArgumentCaptor.forClass(classOf[Identity])
  private def attrCaptor: ArgumentCaptor[BinaryAttributes]      = ArgumentCaptor.forClass(classOf[BinaryAttributes])
  private def instantCaptor: ArgumentCaptor[Instant]            = ArgumentCaptor.forClass(classOf[Instant])

}
