package ch.epfl.bluebrain.nexus.kg.service.instances.attachments

import java.io.File
import java.nio.file.Files

import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.AttachmentLocation
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.AttachmentLocation.Location
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.instances.attachments.RelativeAttachmentLocationSpec._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{Matchers, TryValues, WordSpecLike}

import scala.collection.JavaConverters._
import scala.util.Try
import cats.instances.try_._
import ch.epfl.bluebrain.nexus.kg.core.Fault.Unexpected
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId

class RelativeAttachmentLocationSpec
    extends WordSpecLike
    with Matchers
    with TryValues {

  abstract class Context {
    def config: Config
    val settings = new Settings(config)
    val fa: AttachmentLocation[Try] = RelativeAttachmentLocation[Try](settings)
    val uuid = "017f9837-5bea-4e79-bdbd-e64246cd81ec"
    val rev = 3L
    val instanceId = InstanceId(
      SchemaId(DomainId(OrgId("organization"), "domain"),
               "schema",
               Version(1, 0, 0)),
      uuid.toString)
  }

  "A RelativeAttachmentLocation" should {

    "creates the location of a file correctly" in new Context {
      override def config: Config = correctConfig
      val relativePathExpected = s"0/1/7/f/9/8/3/7/${instanceId.id}.$rev"
      fa.apply(instanceId, rev).success.value shouldEqual
        Location(new File(settings.Attachment.VolumePath.toFile,
                          relativePathExpected).toPath,
                 relativePathExpected)
    }

    "fetches the URI of an attachment from it's relative path" in new Context {
      override def config: Config = correctConfig
      val relativePathExpected = s"0/1/7/f/9/8/3/7/${instanceId.id}.$rev"
      fa.toAbsoluteURI(relativePathExpected) shouldEqual
        new File(settings.Attachment.VolumePath.toFile, relativePathExpected).toURI
    }

    "does not create a location when settings volume-path are incorrect" in new Context {
      override def config: Config = wrongConfig
      fa.apply(instanceId, 3).failure.exception shouldEqual
        Unexpected(
          s"I/O error while trying to create directory for instance '$instanceId'. Error '/0'")
    }
  }
}

object RelativeAttachmentLocationSpec {
  val tempDir = Files.createTempDirectory("atachment").toString
  val correctConfig: Config = ConfigFactory.parseMap(
    Map("app.attachment.volume-path" -> s"$tempDir",
        "app.attachment.digest-algorithm" -> "SHA-256").asJava)

  val wrongConfig: Config = ConfigFactory.parseMap(
    Map("app.attachment.volume-path" -> "",
        "app.attachment.digest-algorithm" -> "SHA-256").asJava)
}
