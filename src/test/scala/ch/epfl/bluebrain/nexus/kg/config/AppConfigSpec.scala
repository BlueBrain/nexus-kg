package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.Paths

import akka.http.scaladsl.model.headers.BasicHttpCredentials
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.concurrent.duration._

class AppConfigSpec extends WordSpecLike with Matchers with OptionValues with TestHelper {

  "An AppConfig" should {
    val valid = ConfigFactory.parseResources("app.conf").resolve()

    "provide the appropriate config" in {
      implicit val appConfig: AppConfig = new Settings(valid).appConfig

      appConfig.description shouldEqual Description("kg")
      appConfig.http shouldEqual HttpConfig("127.0.0.1", 8080, "v1", "http://127.0.0.1:8080")
      appConfig.cluster shouldEqual ClusterConfig(2.seconds, 5 seconds, 30, None)
      appConfig.persistence shouldEqual PersistenceConfig("cassandra-journal",
                                                          "cassandra-snapshot-store",
                                                          "cassandra-query-journal")
      appConfig.storage shouldEqual StorageConfig(DiskStorageConfig(Paths.get("/tmp/"), "SHA-256", read, write),
                                                  S3StorageConfig("MD-5", read, write))
      appConfig.iam shouldEqual IamConfig(url"http://localhost:8080/v1".value,
                                          url"http://localhost:8080/v1".value,
                                          None,
                                          1 second)
      appConfig.sparql shouldEqual SparqlConfig("http://localhost:9999/bigdata", None, None, "kg")
      SparqlConfig("http://localhost:9999/bigdata", Some("user"), Some("pass"), "kg").akkaCredentials.value shouldEqual BasicHttpCredentials(
        "user",
        "pass")
      appConfig.elasticSearch shouldEqual ElasticSearchConfig("http://localhost:9200", "kg", "doc", "kg_default")
      appConfig.pagination shouldEqual PaginationConfig(0L, 20, 100)

      implicitly[SparqlConfig] shouldEqual SparqlConfig("http://localhost:9999/bigdata", None, None, "kg")
      implicitly[ElasticSearchConfig] shouldEqual ElasticSearchConfig("http://localhost:9200",
                                                                      "kg",
                                                                      "doc",
                                                                      "kg_default")
      implicitly[PaginationConfig] shouldEqual PaginationConfig(0L, 20, 100)
      implicitly[PersistenceConfig] shouldEqual PersistenceConfig("cassandra-journal",
                                                                  "cassandra-snapshot-store",
                                                                  "cassandra-query-journal")
    }
  }
}
