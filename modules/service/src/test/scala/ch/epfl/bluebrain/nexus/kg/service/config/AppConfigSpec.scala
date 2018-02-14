package ch.epfl.bluebrain.nexus.kg.service.config

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig._
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class AppConfigSpec extends WordSpecLike with Matchers with ScalatestRouteTest {

  "An AppConfig" should {
    val valid = ConfigFactory.parseResources("test-app.conf").resolve()

    "provide the appropriate config" in {
      val appConfig = new Settings(valid).appConfig

      appConfig.description shouldEqual DescriptionConfig("kg", "local")

      appConfig.instance shouldEqual InstanceConfig("127.0.0.1")

      appConfig.http shouldEqual HttpConfig("127.0.0.1", 8080, "v1", Uri("http://127.0.0.1:8080"))

      appConfig.runtime shouldEqual RuntimeConfig(30 seconds)

      appConfig.cluster shouldEqual ClusterConfig(10 seconds, 100, None)

      appConfig.persistence shouldEqual PersistenceConfig("cassandra-journal",
                                                          "cassandra-snapshot-store",
                                                          "cassandra-query-journal")

      appConfig.projects shouldEqual ProjectsConfig(10 minutes)

      appConfig.schemas shouldEqual SchemasConfig(3 minutes)

      appConfig.instances shouldEqual InstancesConfig(5 seconds, AttachmentConfig(Path("tmp"), "SHA-256"))

      appConfig.prefixes shouldEqual PrefixesConfig(
        "http://127.0.0.1:8080/v1/contexts/nexus/core/resource/v0.3.0",
        "http://127.0.0.1:8080/v1/contexts/nexus/core/standards/v0.1.0",
        "http://127.0.0.1:8080/v1/contexts/nexus/core/links/v0.2.0",
        "http://127.0.0.1:8080/v1/contexts/nexus/core/search/v0.1.0",
        "http://127.0.0.1:8080/v1/contexts/nexus/core/distribution/v0.1.0",
        "http://127.0.0.1:8080/v1/contexts/nexus/core/error/v0.1.0"
      )
      AppConfig.Vocabulary.core shouldEqual Uri(
        "https://bbp-nexus.epfl.ch/vocabs/nexus/core/terms/v0.1.0/createdAtTime")
    }
  }
}
