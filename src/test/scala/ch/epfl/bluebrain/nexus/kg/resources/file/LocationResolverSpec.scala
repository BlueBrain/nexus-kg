package ch.epfl.bluebrain.nexus.kg.resources.file

import java.nio.file.Paths
import java.util.UUID

import cats.effect.IO
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.FileConfig
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.LocationResolver
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.LocationResolver.Location
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class LocationResolverSpec extends WordSpecLike with Matchers with EitherValues {
  "A LocationResolver" should {
    val key = Id(ProjectRef(UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")),
                 url"https://bbp.epfl.ch/nexus/data/resourceName".value)

    "resolve a location" in {
      implicit val config = FileConfig(Paths.get("/tmp"), "SHA-256")
      val resolver        = LocationResolver[IO]()
      val expectedPath =
        Paths.get("4947db1e-33d8-462b-9754-3e8ae74fcd4e/0/1/7/f/9/8/3/7/017f9837-5bea-4e79-bdbd-e64246cd81ec")
      resolver(key, "017f9837-5bea-4e79-bdbd-e64246cd81ec")
        .unsafeRunSync() shouldEqual Location(Paths.get(s"/tmp/$expectedPath"), expectedPath)
    }
  }

}
