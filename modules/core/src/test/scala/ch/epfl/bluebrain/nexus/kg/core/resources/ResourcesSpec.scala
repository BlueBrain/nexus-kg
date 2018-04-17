package ch.epfl.bluebrain.nexus.kg.core.resources

import java.time.Clock

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.core.access.Access._
import ch.epfl.bluebrain.nexus.kg.core.access.HasAccess
import ch.epfl.bluebrain.nexus.kg.core.rejections.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.resources.Payload.JsonPayload
import ch.epfl.bluebrain.nexus.kg.core.resources.ResourceRejection._
import ch.epfl.bluebrain.nexus.kg.core.resources.ResourceType.SchemaType
import ch.epfl.bluebrain.nexus.kg.core.resources.State._
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.{Digest, Info, Size, SourceWrapper}
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment._
import ch.epfl.bluebrain.nexus.kg.core.types.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.types.Project.{Config, ProjectValue}
import ch.epfl.bluebrain.nexus.kg.core.types.{IdVersioned, Project}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{CancelAfterFailure, Matchers, TryValues, WordSpecLike}

import scala.util.{Failure, Success, Try}

class ResourcesSpec
    extends WordSpecLike
    with Matchers
    with TryValues
    with Randomness
    with MockitoSugar
    with CancelAfterFailure {

  private implicit val clock: Clock            = Clock.systemUTC
  private implicit val caller: AnonymousCaller = AnonymousCaller(Anonymous())
  private val agg                              = MemoryAggregate("resource")(Initial, State.next, State.eval).toF[Try]
  private implicit val attachmentStore         = mock[AttachmentStore[Try, String, String]]

  private def genProjectValue: ProjectValue =
    ProjectValue(genString(), Some(genString()), List.empty, Config(genInt().toLong))

  private def genJson: Json = Json.obj("key" -> Json.fromString(genString()))

  trait Context {
    private[resources] def projectDeprecated: Boolean = false
    private[resources] lazy val projectId             = genString()
    private[resources] lazy val id                    = genString()
    private[resources] lazy val schema                = genString()
    private[resources] lazy val project =
      Project(projectId, 1L, genProjectValue, Json.obj(), deprecated = projectDeprecated)
    private[resources] lazy val resources = Resources[SchemaType](agg, project)
    private[resources] lazy val payload   = JsonPayload(genJson)
    private[resources] lazy val attachment =
      Attachment("uri", Info("filename", "mediaType", Size("MB", 123L), Digest("SHA256", "ABCDEF")))
    private[resources] lazy val attMeta =
      SourceWrapper(attachment.name, attachment.info.mediaType, genString())
    private[resources] lazy val attachment2 =
      Attachment("uri", Info("filename2", "mediaType2", Size("MB", 3L), Digest("SHA256", "ABCDEF")))
    private[resources] lazy val att2Meta =
      SourceWrapper(attachment2.name, attachment2.info.mediaType, genString())

    private[resources] def reprId(id: String, schema: String) = RepresentationId(projectId, id, schema)
  }

  "A Resource" should {

    "be created correctly" in new Context {
      private implicit val access: SchemaType HasAccess Create = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
    }

    "be replaced correctly" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val write: SchemaType HasAccess Write   = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.replace(id, schema, 1L, JsonPayload(genJson)).success.value shouldEqual IdVersioned(reprId(id, schema),
                                                                                                    2L)
    }

    "reject to be replaced when revision is incorrect" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val write: SchemaType HasAccess Write   = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.replace(id, schema, 3L, payload).failure shouldEqual Failure(CommandRejected(IncorrectRevisionProvided))
    }

    "reject to be replaced when id does not exists" in new Context {
      private implicit val write: SchemaType HasAccess Write = null
      resources.replace(id, schema, 1L, payload).failure shouldEqual Failure(CommandRejected(ResourceDoesNotExists))
    }

    "be fetched correctly" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      private val replacedPayload                              = JsonPayload(genJson)
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.replace(id, schema, 1L, replacedPayload).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)

      resources.fetch(id, schema).success.value shouldEqual Some(
        Resource(reprId(id, schema), 2L, replacedPayload, Set.empty, deprecated = false))
      resources.fetch(id, schema, 1L).success.value shouldEqual Some(
        Resource(reprId(id, schema), 1L, payload, Set.empty, deprecated = false))
    }

    "return None when fetching a non-existing revision" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val read: SchemaType HasAccess Read     = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.fetch(id, schema, 10L).success.value shouldEqual None
    }

    "be deprecated correctly" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.deprecate(id, schema, 1L).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.fetch(id, schema).success.value shouldEqual Some(
        Resource(reprId(id, schema), 2L, payload, Set.empty, deprecated = true))

    }

    "reject to be deprecated twice" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val write: SchemaType HasAccess Write   = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.deprecate(id, schema, 1L).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.deprecate(id, schema, 2L).failure shouldEqual Failure(CommandRejected(ResourceIsDeprecated))
    }

    "reject to be replaced if it is deprecated" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val write: SchemaType HasAccess Write   = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.deprecate(id, schema, 1L).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.replace(id, schema, 2L, JsonPayload(genJson)).failure shouldEqual Failure(
        CommandRejected(ResourceIsDeprecated))
    }

    "reject to be deprecated when wrong revision provided" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val write: SchemaType HasAccess Write   = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.deprecate(id, schema, 3L).failure shouldEqual Failure(CommandRejected(IncorrectRevisionProvided))
    }

    "be undeprecated correctly" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.deprecate(id, schema, 1L).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.undeprecate(id, schema, 2L).success.value shouldEqual IdVersioned(reprId(id, schema), 3L)
      resources.fetch(id, schema).success.value shouldEqual Some(
        Resource(reprId(id, schema), 3L, payload, Set.empty, deprecated = false))
    }

    "reject to be undeprecated when wrong revision provided" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val write: SchemaType HasAccess Write   = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.deprecate(id, schema, 1L).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.undeprecate(id, schema, 4L).failure shouldEqual Failure(CommandRejected(IncorrectRevisionProvided))
    }

    "reject to be undeprecated when it is not deprecated" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val write: SchemaType HasAccess Write   = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.undeprecate(id, schema, 1L).failure shouldEqual Failure(CommandRejected(ResourceIsNotDeprecated))
    }

    "be tagged and fetched by tag correctly" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      private val replacedPayload                              = JsonPayload(genJson)
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.replace(id, schema, 1L, replacedPayload).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.tag(id, schema, 1L, "tag1").success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.tag(id, schema, 2L, "tag2").success.value shouldEqual IdVersioned(reprId(id, schema), 2L)

      resources.fetch(id, schema, "tag1").success.value shouldEqual Some(
        Resource(reprId(id, schema), 1L, payload, Set.empty, deprecated = false))
      resources.fetch(id, schema, "tag2").success.value shouldEqual Some(
        Resource(reprId(id, schema), 2L, replacedPayload, Set.empty, deprecated = false))
    }

    "reject to be tagged when wrong revision provided" in new Context {
      private implicit val create: SchemaType HasAccess Create = null
      private implicit val write: SchemaType HasAccess Write   = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.tag(id, schema, 5L, "tag1").failure shouldEqual Failure(CommandRejected(IncorrectRevisionProvided))
    }

    "return None when tag not found" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.tag(id, schema, 1L, "tag1").success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.fetch(id, schema, "tag2").success.value shouldEqual None
    }

    "be tagged even when deprecated" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.deprecate(id, schema, 1L).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.tag(id, schema, 1L, "tag1").success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.fetch(id, schema, "tag1").success.value shouldEqual Some(
        Resource(reprId(id, schema), 1L, payload, Set.empty, deprecated = false))
    }

    "add attachments correctly" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      when(attachmentStore.save(reprId(id, schema), 1L, attMeta)).thenReturn(Success(attachment))
      resources.attach(id, schema, 1L, attMeta).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      when(attachmentStore.save(reprId(id, schema), 2L, att2Meta)).thenReturn(Success(attachment2))
      resources.attach(id, schema, 2L, att2Meta).success.value shouldEqual IdVersioned(reprId(id, schema), 3L)
      resources.fetch(id, schema).success.value shouldEqual Some(
        Resource(reprId(id, schema), 3L, payload, Set(attachment, attachment2), deprecated = false))
      resources.fetch(id, schema, 2L).success.value shouldEqual Some(
        Resource(reprId(id, schema), 2L, payload, Set(attachment), deprecated = false))
    }

    "fetch attachments" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      when(attachmentStore.save(reprId(id, schema), 1L, attMeta)).thenReturn(Success(attachment))
      resources.attach(id, schema, 1L, attMeta).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      when(attachmentStore.save(reprId(id, schema), 2L, att2Meta)).thenReturn(Success(attachment2))
      resources.attach(id, schema, 2L, att2Meta).success.value shouldEqual IdVersioned(reprId(id, schema), 3L)
      resources.fetch(id, schema).success.value shouldEqual Some(
        Resource(reprId(id, schema), 3L, payload, Set(attachment, attachment2), deprecated = false))
      resources.fetch(id, schema, 2L).success.value shouldEqual Some(
        Resource(reprId(id, schema), 2L, payload, Set(attachment), deprecated = false))
      resources.fetchAttachment(id, schema, "non-existing").success.value shouldEqual None

      when(attachmentStore.fetch(attachment)).thenReturn(Success(attMeta.source))

      resources.fetchAttachment(id, schema, attachment.name).success.value shouldEqual Some(
        attachment.info -> attMeta.source)

      resources.fetchAttachment(id, schema, 2L, attachment.name).success.value shouldEqual Some(
        attachment.info -> attMeta.source)

      resources.tag(id, schema, 3L, "tag1").success.value shouldEqual IdVersioned(reprId(id, schema), 3L)

      resources.fetchAttachment(id, schema, "tag1", attachment.name).success.value shouldEqual Some(
        attachment.info -> attMeta.source)

    }

    "reject to add attachments when deprecated" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      resources.deprecate(id, schema, 1L).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.attach(id, schema, 2L, att2Meta).failure shouldEqual Failure(CommandRejected(ResourceIsDeprecated))
    }

    "remove attachments correctly" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      when(attachmentStore.save(reprId(id, schema), 1L, attMeta)).thenReturn(Success(attachment))
      resources.attach(id, schema, 1L, attMeta).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      when(attachmentStore.save(reprId(id, schema), 2L, att2Meta)).thenReturn(Success(attachment2))
      resources.attach(id, schema, 2L, att2Meta).success.value shouldEqual IdVersioned(reprId(id, schema), 3L)
      resources.unattach(id, schema, 3L, attachment.name).success.value shouldEqual IdVersioned(reprId(id, schema), 4L)
      resources.fetch(id, schema).success.value shouldEqual Some(
        Resource(reprId(id, schema), 4L, payload, Set(attachment2), deprecated = false))
    }

    "reject remove attachments when deprecated" in new Context {
      private implicit val manage: SchemaType HasAccess Manage = null
      resources.create(id, schema, payload).success.value shouldEqual IdVersioned(reprId(id, schema), 1L)
      when(attachmentStore.save(reprId(id, schema), 1L, attMeta)).thenReturn(Success(attachment))
      resources.attach(id, schema, 1L, attMeta).success.value shouldEqual IdVersioned(reprId(id, schema), 2L)
      resources.deprecate(id, schema, 2L).success.value shouldEqual IdVersioned(reprId(id, schema), 3L)
      resources.unattach(id, schema, 3L, attachment.name).failure shouldEqual Failure(
        CommandRejected(ResourceIsDeprecated))
    }

    "reject creation when project is deprecated" in new Context {
      override private[resources] def projectDeprecated        = true
      private implicit val create: SchemaType HasAccess Create = null
      resources.create(id, schema, payload).failure shouldEqual Failure(CommandRejected(ParentResourceIsDeprecated))
    }

    "reject replacement when project is deprecated" in new Context {
      override private[resources] def projectDeprecated      = true
      private implicit val write: SchemaType HasAccess Write = null
      resources.replace(id, schema, 1L, payload).failure shouldEqual Failure(
        CommandRejected(ParentResourceIsDeprecated))
    }

    "reject attachment when project is deprecated" in new Context {
      override private[resources] def projectDeprecated        = true
      private implicit val attach: SchemaType HasAccess Attach = null
      resources.attach(id, schema, 1L, attMeta).failure shouldEqual Failure(CommandRejected(ParentResourceIsDeprecated))
    }

    "reject attachment removal when project is deprecated" in new Context {
      override private[resources] def projectDeprecated        = true
      private implicit val attach: SchemaType HasAccess Attach = null
      resources.unattach(id, schema, 1L, attachment.name).failure shouldEqual Failure(
        CommandRejected(ParentResourceIsDeprecated))
    }

    "reject deprecation when project is deprecated" in new Context {
      override private[resources] def projectDeprecated      = true
      private implicit val write: SchemaType HasAccess Write = null
      resources.deprecate(id, schema, 1L).failure shouldEqual Failure(CommandRejected(ParentResourceIsDeprecated))
    }

    "reject undeprecation when project is deprecated" in new Context {
      override private[resources] def projectDeprecated      = true
      private implicit val write: SchemaType HasAccess Write = null
      resources.undeprecate(id, schema, 1L).failure shouldEqual Failure(CommandRejected(ParentResourceIsDeprecated))
    }
  }
}
