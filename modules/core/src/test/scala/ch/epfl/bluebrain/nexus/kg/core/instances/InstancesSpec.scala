package ch.epfl.bluebrain.nexus.kg.core.instances

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.security.MessageDigest
import java.time.Clock
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.regex.Pattern.quote

import cats.Show
import cats.instances.try_._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidator
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.AggregatedImportResolver
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRejection._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstancesSpec._
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment.{Digest, Info, Size}
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.{Attachment, AttachmentLocation, InOutFileStream}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaImportResolver, SchemaRejection, Schemas}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, TryValues, WordSpecLike}

import scala.util.Try

//noinspection TypeAnnotation
class InstancesSpec extends WordSpecLike with Matchers with Inspectors with TryValues with Randomness with Resources {

  def genId(): String =
    genString(length = 4, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  def genJson(): Json =
    jsonContentOf("/int-value.json").deepMerge(Json.obj("value" -> Json.fromInt(genInt(Int.MaxValue))))

  def genName(): String =
    genString(length = 8, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  def genUUID(): String =
    UUID.randomUUID().toString.toLowerCase

  val schemaJson = jsonContentOf("/int-value-schema.json")
  val baseUri    = "http://localhost:8080/v0"

  private implicit val caller = AnonymousCaller(Anonymous())
  private implicit val clock  = Clock.systemUTC

  abstract class Context {
    implicit val al = new MockedAttachmentLocation()
    val orgsAgg     = MemoryAggregate("org")(Organizations.initial, Organizations.next, Organizations.eval).toF[Try]
    val domAgg =
      MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval)
        .toF[Try]
    val schemasAgg =
      MemoryAggregate("schema")(Schemas.initial, Schemas.next, Schemas.eval)
        .toF[Try]
    val ctxsAgg =
      MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval)
        .toF[Try]
    val instAgg         = MemoryAggregate("instance")(Instances.initial, Instances.next, Instances.eval).toF[Try]
    val inOutFileStream = new MockedInOutFileStream()

    val orgs      = Organizations(orgsAgg)
    val doms      = Domains(domAgg, orgs)
    val ctxs      = Contexts(ctxsAgg, doms, baseUri)
    val schemas   = Schemas(schemasAgg, doms, ctxs)
    val instances = Instances(instAgg, schemas, ctxs, inOutFileStream)

    val schemaImportResolver            = new SchemaImportResolver(baseUri, schemas.fetch, ctxs.resolve)
    implicit val instanceImportResolver = new InstanceImportResolver[Try](baseUri, instances.fetch, ctxs.resolve)
    implicit val validator: ShaclValidator[Try] =
      new ShaclValidator[Try](AggregatedImportResolver(schemaImportResolver, instanceImportResolver))

    val orgRef = orgs.create(OrgId(genId()), genJson()).success.value
    val domRef =
      doms.create(DomainId(orgRef.id, genId()), "domain").success.value
    private val unpublished = schemas
      .create(SchemaId(domRef.id, genName(), genVersion()), schemaJson)
      .success
      .value
    val schemaRef =
      schemas.publish(unpublished.id, unpublished.rev).success.value

    private val ctx =
      ctxs.create(ContextId(domRef.id, genName(), genVersion()), jsonContentOf("/contexts/shacl.json")).success.value
    private val _ = ctxs.publish(ctx.id, ctx.rev).success
    val ctxReplacements = Map(
      quote("{{context}}") -> s"$baseUri/contexts/${ctx.id.show}"
    )
    private val qvalue = schemas
      .create(SchemaId(domRef.id, genName(), genVersion()),
              jsonContentOf("/importing-int-value-schema.json", ctxReplacements))
      .success
      .value
    val qvalueRef = schemas.publish(qvalue.id, qvalue.rev).success.value

    def createAttachmentRequest(id: InstanceId,
                                rev: Long,
                                source: String,
                                filename: String,
                                contentType: String): (Attachment.Info, Try[InstanceRef]) = {
      val expectedInfo = Attachment.Info(filename,
                                         contentType,
                                         Size(value = source.length.toLong),
                                         Digest("SHA-256", digestString(source)))
      expectedInfo -> instances.createAttachment(id, rev, expectedInfo.originalFileName, expectedInfo.mediaType, source)
    }
  }

  "An Instances" should {

    "create a new instance" in new Context {
      val id    = InstanceId(schemaRef.id, genUUID())
      val value = genJson()
      instances.create(id, value).success.value shouldEqual InstanceRef(id, 1L)
      instances.fetch(id).success.value shouldEqual Some(Instance(id, 1L, value, deprecated = false))
    }

    "create a new instance with a generated id" in new Context {
      val value = genJson()
      val ref   = instances.create(schemaRef.id, value).success.value
      ref shouldBe an[InstanceRef]
      instances.fetch(ref.id).success.value shouldEqual Some(Instance(ref.id, 1L, value, deprecated = false))
    }

    "update an instance" in new Context {
      val id     = InstanceId(schemaRef.id, genUUID())
      val value1 = genJson()
      val value2 = genJson()
      instances.create(id, value1).success.value shouldEqual InstanceRef(id, 1L)
      instances.update(id, 1L, value2).success.value shouldEqual InstanceRef(id, 2L)
      instances.fetch(id).success.value shouldEqual Some(Instance(id, 2L, value2, deprecated = false))
    }

    "deprecate an instance" in new Context {
      val id    = InstanceId(schemaRef.id, genUUID())
      val value = genJson()
      instances.create(id, value).success.value shouldEqual InstanceRef(id, 1L)
      instances.deprecate(id, 1L).success.value shouldEqual InstanceRef(id, 2L)
      instances.fetch(id).success.value shouldEqual Some(Instance(id, 2L, value, deprecated = true))
    }

    "prevent double deprecations" in new Context {
      val id    = InstanceId(schemaRef.id, genUUID())
      val value = genJson()
      instances.create(id, value).success.value shouldEqual InstanceRef(id, 1L)
      instances.deprecate(id, 1L).success.value shouldEqual InstanceRef(id, 2L)
      instances.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(InstanceIsDeprecated)
    }

    "prevent update when deprecated" in new Context {
      val id    = InstanceId(schemaRef.id, genUUID())
      val value = genJson()
      instances.create(id, value).success.value shouldEqual InstanceRef(id, 1L)
      instances.deprecate(id, 1L).success.value shouldEqual InstanceRef(id, 2L)
      instances
        .update(id, 2L, genJson())
        .failure
        .exception shouldEqual CommandRejected(InstanceIsDeprecated)
    }

    "prevent update with incorrect rev" in new Context {
      val id    = InstanceId(schemaRef.id, genUUID())
      val value = genJson()
      instances.create(id, value).success.value shouldEqual InstanceRef(id, 1L)
      instances
        .update(id, 2L, genJson())
        .failure
        .exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "prevent deprecate with incorrect rev" in new Context {
      val id    = InstanceId(schemaRef.id, genUUID())
      val value = genJson()
      instances.create(id, value).success.value shouldEqual InstanceRef(id, 1L)
      instances.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "return None for an instance that doesn't exist" in new Context {
      val id = InstanceId(schemaRef.id, genUUID())
      instances.fetch(id).success.value shouldEqual None
    }

    "prevent creation if schema is locked" in new Context {
      val id = InstanceId(schemaRef.id, genUUID())
      schemas.deprecate(schemaRef.id, schemaRef.rev).success
      instances
        .create(id, genJson())
        .failure
        .exception shouldEqual CommandRejected(SchemaRejection.SchemaIsDeprecated)
    }

    "prevent creation if schema is not published" in new Context {
      val unpublished = schemas
        .create(SchemaId(domRef.id, genName(), genVersion()), schemaJson)
        .success
        .value
      val id = InstanceId(unpublished.id, genUUID())
      instances
        .create(id, genJson())
        .failure
        .exception shouldEqual CommandRejected(SchemaRejection.SchemaIsNotPublished)
    }

    "allow update if schema is deprecated" in new Context {
      val id = InstanceId(schemaRef.id, genUUID())
      instances.create(id, genJson()).success.value shouldEqual InstanceRef(id, 1L)
      schemas.deprecate(schemaRef.id, schemaRef.rev).success
      instances.update(id, 1L, genJson()).success.value shouldEqual InstanceRef(id, 2L)
    }

    "prevent creation if the json value does not conform to its schema" in new Context {
      val id    = InstanceId(schemaRef.id, genUUID())
      val value = Json.obj()
      instances.create(id, value).failure.exception match {
        case CommandRejected(ShapeConstraintViolations(vs)) =>
          vs should not be 'empty
        case _ => fail("instance creation with invalid data was not rejected")
      }
    }

    "allow instance creation when the schema and instance refer to an external context" in new Context {
      val id    = InstanceId(qvalueRef.id, genUUID())
      val value = jsonContentOf("/importing-int-value.json", ctxReplacements)
      instances.create(id, value).success
    }

    "prevent update if the new json value does not conform to its schema" in new Context {
      val id           = InstanceId(schemaRef.id, genUUID())
      val value        = genJson()
      val updatedValue = Json.obj()
      instances.create(id, value).success.value shouldEqual InstanceRef(id, 1L)
      instances.update(id, 1L, updatedValue).failure.exception match {
        case CommandRejected(ShapeConstraintViolations(vs)) =>
          vs should not be 'empty
        case _ => fail("instance update with invalid data was not rejected")
      }
      instances.fetch(id).success.value shouldEqual Some(Instance(id, 1L, value, deprecated = false))
    }
    "create a new instance attachment" in new Context {
      val id           = InstanceId(schemaRef.id, genUUID())
      private val json = genJson()
      instances.create(id, json).success.value shouldEqual InstanceRef(id, 1L)
      val source = "some_random_content"
      val (expectedInfo, response) =
        createAttachmentRequest(id, 1, source, "filename", "application/octet-stream")
      response.success.value shouldEqual InstanceRef(id, 2L, Some(expectedInfo))
      instances.fetch(id).success.value shouldEqual Some(Instance(id, 2L, json, Some(expectedInfo), deprecated = false))
    }

    "create a new instance attachment and later update an instance should keep attachment" in new Context {
      val id     = InstanceId(schemaRef.id, genUUID())
      val value1 = genJson()
      val value2 = genJson()
      instances.create(id, value1).success.value shouldEqual InstanceRef(id, 1L)

      val source = "some_random_content"
      val (expectedInfo, response) =
        createAttachmentRequest(id, 1, source, "filename", "application/octet-stream")
      response.success.value shouldEqual InstanceRef(id, 2L, Some(expectedInfo))

      instances.update(id, 2L, value2).success.value shouldEqual InstanceRef(id, 3L)
      instances.fetch(id).success.value shouldEqual Some(
        Instance(id, 3L, value2, Some(expectedInfo), deprecated = false))
    }

    "fetch a new instance attachment" in new Context {
      val id           = InstanceId(schemaRef.id, genUUID())
      private val json = genJson()
      instances.create(id, json).success.value shouldEqual InstanceRef(id, 1L)
      val source = "some_random_content"
      val (expectedInfo, response) =
        createAttachmentRequest(id, 1, source, "filename", "application/octet-stream")
      response.success.value shouldEqual InstanceRef(id, 2L, Some(expectedInfo))
      instances.fetchAttachment(id).success.value shouldEqual Some(expectedInfo -> source)
    }

    "prevent fetching an attachment when instance doesn't exists" in new Context {
      val id           = InstanceId(schemaRef.id, genUUID())
      private val json = genJson()
      instances.create(id, json).success.value shouldEqual InstanceRef(id, 1L)
      instances.fetchAttachment(id).success.value shouldEqual None
    }

    "create several instance attachments, delete afterwards and fetch and older revisions" in new Context {
      val id           = InstanceId(schemaRef.id, genUUID())
      private val json = genJson()
      instances.create(id, json).success.value shouldEqual InstanceRef(id, 1L)
      val source = "some_random_content"
      val (expectedInfo, response) =
        createAttachmentRequest(id, 1, source, "filename", "application/octet-stream")
      response.success.value shouldEqual InstanceRef(id, 2L, Some(expectedInfo))

      val source2 = """{"one": "two"}"""
      val (expectedInfo2, response2) =
        createAttachmentRequest(id, 2, source2, "filename2", "application/octet-stream")
      response2.success.value shouldEqual InstanceRef(id, 3L, Some(expectedInfo2))

      instances.removeAttachment(id, 3L).success.value shouldEqual InstanceRef(id, 4L)

      instances.fetch(id, 2L).success.value shouldEqual Some(
        Instance(id, 2L, json, Some(expectedInfo), deprecated = false))
      instances.fetch(id, 3L).success.value shouldEqual Some(
        Instance(id, 3L, json, Some(expectedInfo2), deprecated = false))
      instances.fetch(id).success.value shouldEqual Some(Instance(id, 4L, json, None, deprecated = false))

    }

    "prevent creation of attachment with incorrect rev" in new Context {
      val id           = InstanceId(schemaRef.id, genUUID())
      private val json = genJson()
      instances.create(id, json).success.value shouldEqual InstanceRef(id, 1L)
      val source = "some_random_content"
      instances
        .createAttachment(id, 4, "filename", "application/octet-stream", source)
        .failure
        .exception shouldEqual CommandRejected(IncorrectRevisionProvided)
      instances.fetch(id).success.value shouldEqual Some(Instance(id, 1L, json, None, deprecated = false))
    }

    "prevent creation of attachment with instance that doesn't exist" in new Context {
      val id     = InstanceId(schemaRef.id, genUUID())
      val source = "some_random_content"
      instances
        .createAttachment(id, 1, "filename", "application/octet-stream", source)
        .failure
        .exception shouldEqual CommandRejected(InstanceDoesNotExist)
    }

    "remove a new instance attachment" in new Context {
      val id           = InstanceId(schemaRef.id, genUUID())
      private val json = genJson()
      instances.create(id, json).success.value shouldEqual InstanceRef(id, 1L)

      val source = "some_random_content"
      val (expectedInfo, response) =
        createAttachmentRequest(id, 1, source, "filename", "application/octet-stream")
      response.success.value shouldEqual InstanceRef(id, 2L, Some(expectedInfo))

      instances.removeAttachment(id, 2L).success.value shouldEqual InstanceRef(id, 3L)
      instances.fetch(id).success.value shouldEqual Some(Instance(id, 3L, json, None, deprecated = false))
    }

    "prevent to remove an instance attachment when it does not exist" in new Context {
      val id = InstanceId(schemaRef.id, genUUID())
      instances.create(id, genJson()).success.value shouldEqual InstanceRef(id, 1L)
      instances
        .removeAttachment(id, 1L)
        .failure
        .exception shouldEqual CommandRejected(AttachmentNotFound)
    }
  }
}

object InstancesSpec {
  val base: File = Files.createTempDirectory("attachment").toFile

  def digestString(string: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    digest.digest(string.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  class MockedAttachmentLocation extends AttachmentLocation[Try] {

    override def toAbsoluteURI(relative: String): URI =
      new File(base, relative).toURI

    override def apply(instanceId: InstanceId, rev: Long): Try[AttachmentLocation.Location] = {
      val relativeRoute = s"${Show[InstanceId].show(instanceId)}-$rev"
      val path          = new File(base, relativeRoute).toPath
      Try(AttachmentLocation.Location(path, relativeRoute))
    }
  }

  class MockedInOutFileStream(implicit S: Show[InstanceId]) extends InOutFileStream[Try, String, String] {
    private final val map = new ConcurrentHashMap[URI, String]()

    override def toSource(uri: URI): Try[String] = Try(map.get(uri))

    override def toSink(instanceId: InstanceId,
                        rev: Long,
                        filename: String,
                        contentType: String,
                        source: String): Try[Attachment.Meta] =
      Try {
        val relativeRoute = s"${Show[InstanceId].show(instanceId)}-$rev"
        val uri           = new File(base, relativeRoute).toURI
        map.put(uri, source)
        Attachment.Meta(
          relativeRoute,
          Info(filename, contentType, Size(value = source.length.toLong), Digest("SHA-256", digestString(source))))
      }
  }
}
