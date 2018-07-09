package ch.epfl.bluebrain.nexus.kg.core.config

import java.io.File

import akka.http.scaladsl.model.Uri
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

class AppConfigSpec extends WordSpecLike with Matchers {

  private val valid = ConfigFactory.parseResources("test-settings.conf").resolve()

  "A pureconfig extension" should {
    "provide the appropriate config" in {
      implicit val appConfig = Settings(valid).appConfig

      appConfig.description shouldEqual DescriptionConfig("kg")

      appConfig.instance shouldEqual InstanceConfig("127.1.2.3")

      appConfig.http shouldEqual HttpConfig("127.1.2.3", 8080, "v1", "http://localhost:8080")

      appConfig.runtime shouldEqual RuntimeConfig(30 seconds)

      appConfig.cluster shouldEqual ClusterConfig(10 seconds, 100, Some("seed1,seed2,seed3"))

      appConfig.persistence shouldEqual PersistenceConfig("cassandra-journal",
                                                          "cassandra-snapshot-store",
                                                          "cassandra-query-journal")

      appConfig.pagination shouldEqual PaginationConfig(0L, 10, 50)

      val attachmentConfig = AttachmentConfig(new File("/tmp").toPath, "SHA-256")
      appConfig.attachment shouldEqual attachmentConfig
      implicitly[AttachmentConfig] shouldEqual attachmentConfig

      appConfig.admin shouldEqual AdminConfig("http://localhost:8080/admin", "projects")
      implicitly[AdminConfig] shouldEqual AdminConfig("http://localhost:8080/admin", "projects")

      appConfig.prefixes shouldEqual PrefixesConfig(
        Iri.absolute("http://localhost:8080/v1/contexts/nexus/core/resource/v0.1.0").right.value,
        Iri.absolute("http://localhost:8080/v1/contexts/nexus/core/standards/v0.1.0").right.value,
        Iri.absolute("http://localhost:8080/v1/contexts/nexus/core/links/v0.1.0").right.value,
        Iri.absolute("http://localhost:8080/v1/contexts/nexus/core/search/v0.1.0").right.value,
        Iri.absolute("http://localhost:8080/v1/contexts/nexus/core/distribution/v0.1.0").right.value,
        Iri.absolute("http://localhost:8080/v1/contexts/nexus/core/error/v0.1.0").right.value,
        Uri("http://localhost:8080/vocabs/nexus/core/terms/v0.1.0/")
      )
    }
  }
}
