package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.Paths

import akka.http.scaladsl.model.headers.BasicHttpCredentials
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig
import ch.epfl.bluebrain.nexus.sourcing.projections.IndexingConfig
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
      appConfig.storage shouldEqual StorageConfig(
        DiskStorageConfig(Paths.get("/tmp/"), "SHA-256", read, write),
        ExternalDiskStorageConfig("http://localhost:8084/v1", None, "SHA-256", read, write),
        S3StorageConfig("SHA-256", read, write),
        "changeme",
        "salt"
      )
      appConfig.iam shouldEqual IamConfig(url"http://localhost:8080/v1".value,
                                          url"http://localhost:8080/v1".value,
                                          None,
                                          1 second)
      val retryIndex = RetryStrategyConfig("exponential", 100 millis, 10 minutes, 7, 0.2, 500 millis)
      val retryQuery = RetryStrategyConfig("exponential", 100 millis, 1 minute, 4, 0.2, 500 millis)
      appConfig.sparql shouldEqual SparqlConfig("http://localhost:9999/bigdata",
                                                "kg",
                                                None,
                                                None,
                                                "kg",
                                                IndexingConfig(10, 300 millis, retryIndex),
                                                retryQuery)
      appConfig.sparql
        .copy(username = Some("user"), password = Some("pass"))
        .akkaCredentials
        .value shouldEqual BasicHttpCredentials("user", "pass")
      appConfig.elasticSearch shouldEqual ElasticSearchConfig("http://localhost:9200",
                                                              "kg",
                                                              "doc",
                                                              "kg_default",
                                                              IndexingConfig(30, 300 millis, retryIndex),
                                                              retryQuery)
      appConfig.pagination shouldEqual PaginationConfig(0, 20, 100)

      implicitly[SparqlConfig] shouldEqual SparqlConfig("http://localhost:9999/bigdata",
                                                        "kg",
                                                        None,
                                                        None,
                                                        "kg",
                                                        IndexingConfig(10, 300 millis, retryIndex),
                                                        retryQuery)
      implicitly[ElasticSearchConfig] shouldEqual ElasticSearchConfig("http://localhost:9200",
                                                                      "kg",
                                                                      "doc",
                                                                      "kg_default",
                                                                      IndexingConfig(30, 300 millis, retryIndex),
                                                                      retryQuery)
      implicitly[PaginationConfig] shouldEqual PaginationConfig(0, 20, 100)
      implicitly[PersistenceConfig] shouldEqual PersistenceConfig("cassandra-journal",
                                                                  "cassandra-snapshot-store",
                                                                  "cassandra-query-journal")
    }
  }
}
