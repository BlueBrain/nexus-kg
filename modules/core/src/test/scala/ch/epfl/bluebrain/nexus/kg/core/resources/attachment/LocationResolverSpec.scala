package ch.epfl.bluebrain.nexus.kg.core.resources.attachment

import java.io.File
import java.net.URI
import java.nio.file.Files

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.kg.core.config.AppConfig.AttachmentConfig
import ch.epfl.bluebrain.nexus.kg.core.resources.RepresentationId
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.LocationResolver.Location
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
      val reprId   = RepresentationId("project", "http://localhost/data/id", "http://localhost/schema/id")
      val filename = "file.txt"
      location(reprId, 1L, filename).success.value shouldEqual Location(
        new File(config.volume.toFile, s"project/${reprId.persId}/1-file.txt").toPath,
        s"project/${reprId.persId}/1-file.txt")
    }
  }

}
