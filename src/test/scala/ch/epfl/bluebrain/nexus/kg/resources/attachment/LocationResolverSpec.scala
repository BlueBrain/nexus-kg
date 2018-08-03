package ch.epfl.bluebrain.nexus.kg.resources.attachment

import java.io.File

import cats.data.EitherT
import cats.syntax.show._
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.AttachmentsConfig
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.LocationResolver
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.LocationResolver.Location
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class LocationResolverSpec extends WordSpecLike with Matchers with EitherValues {
  "A LocationResolver" should {
    val key = Id(ProjectRef("org/projectName"), url"https://bbp.epfl.ch/nexus/data/resourceName".value)

    "resolve a location" in {
      implicit val config = AttachmentsConfig(Iri.absolute("file:///tmp").right.value, "SHA-256")
      val resolver        = LocationResolver[CId]()
      val expectedPath =
        Iri.relative("org/projectName/0/1/7/f/9/8/3/7/017f9837-5bea-4e79-bdbd-e64246cd81ec").right.value
      resolver(key, "017f9837-5bea-4e79-bdbd-e64246cd81ec") shouldEqual EitherT.rightT(
        Location(new File(s"file:/tmp/${expectedPath.show}").toPath, expectedPath))
    }
  }

}
