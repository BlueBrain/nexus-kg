package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import java.io.File
import java.net.URI
import java.nio.file.Files

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AttachmentConfig
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment.BinaryDescription
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.LocationResolver.Location
import eu.timepit.refined.auto._
import org.scalatest.{Matchers, TryValues, WordSpecLike}

import scala.util.Try

class LocationResolverSpec extends WordSpecLike with Matchers with TryValues {
  "A LocationResolver" should {
    implicit val config = AttachmentConfig(Files.createTempDirectory("test").toAbsolutePath, "SHA-1")
    val location        = LocationResolver[Try]

    "generate a URI from a base and the relative path" in {
      location.absoluteUri("one-two-three") shouldEqual new URI(s"file:${config.volume}/one-two-three")
    }

    "generate a location from a root path" in {
      location("project", BinaryDescription("4235902c-4236-11e8-842f-0ed5f89f718b", "file.txt", "text/plain")).success.value shouldEqual Location(
        new File(config.volume.toFile, "project/4/2/3/5/9/0/2/c/4235902c-4236-11e8-842f-0ed5f89f718b").toPath,
        "project/4/2/3/5/9/0/2/c/4235902c-4236-11e8-842f-0ed5f89f718b"
      )
    }
  }

}
