package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.Paths

import akka.http.scaladsl.model.headers.BasicHttpCredentials
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, OptionValues, WordSpecLike}

import scala.concurrent.duration._

class AppConfigSpec extends WordSpecLike with Matchers with OptionValues {

  "An AppConfig" should {
    val valid = ConfigFactory.parseResources("app.conf").resolve()

    "provide the appropriate config" in {
      implicit val appConfig = new Settings(valid).appConfig

      appConfig.description shouldEqual Description("kg")
      appConfig.http shouldEqual HttpConfig("127.0.0.1", 8080, "v1", "http://127.0.0.1:8080")
      appConfig.cluster shouldEqual ClusterConfig(2.seconds, 5.seconds, 30, None)
      appConfig.persistence shouldEqual PersistenceConfig("cassandra-journal",
                                                          "cassandra-snapshot-store",
                                                          "cassandra-query-journal")
      appConfig.attachments shouldEqual AttachmentsConfig(Paths.get("/tmp/"), "SHA-256")
      appConfig.iam shouldEqual IamConfig("http://localhost:8080/iam", None)
      appConfig.sparql shouldEqual SparqlConfig("http://localhost:9999/bigdata", None, None, "kg")
      SparqlConfig("http://localhost:9999/bigdata", Some("user"), Some("pass"), "kg").akkaCredentials.value shouldEqual BasicHttpCredentials(
        "user",
        "pass")
      appConfig.elastic shouldEqual ElasticConfig("http://localhost:9200", "kg", "doc", "kg_default")
      appConfig.pagination shouldEqual PaginationConfig(0L, 20, 100)
      appConfig.kafka.adminTopic shouldEqual "admin-events"
      appConfig.kafka.migration.enabled shouldEqual false
      appConfig.kafka.migration.topic shouldEqual "v0-events"
      appConfig.kafka.migration.baseUri shouldEqual url"http://bbp-nexus.epfl.ch/staging/v0/".value
      appConfig.kafka.migration.projectRef shouldEqual ProjectRef("bbp/v0")
      appConfig.kafka.migration.pattern.toString shouldEqual "bbp/.*"

      implicitly[SparqlConfig] shouldEqual SparqlConfig("http://localhost:9999/bigdata", None, None, "kg")
      implicitly[ElasticConfig] shouldEqual ElasticConfig("http://localhost:9200", "kg", "doc", "kg_default")
      implicitly[PaginationConfig] shouldEqual PaginationConfig(0L, 20, 100)
      implicitly[PersistenceConfig] shouldEqual PersistenceConfig("cassandra-journal",
                                                                  "cassandra-snapshot-store",
                                                                  "cassandra-query-journal")
    }
  }
}
