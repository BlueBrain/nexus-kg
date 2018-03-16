package ch.epfl.bluebrain.nexus.kg.service.config

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.http.ContextUri
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class AppConfigSpec extends WordSpecLike with Matchers {

  private val valid = ConfigFactory.parseResources("test-settings.conf").resolve()

  "A pureconfig extension" should {
    "provide the appropriate config" in {
      val appConfig = new Settings(valid).appConfig

      appConfig.description.name shouldEqual "kg"

      appConfig.instance.interface shouldEqual "127.1.2.3"

      appConfig.http.interface shouldEqual "127.1.2.3"
      appConfig.http.port shouldEqual 8080
      appConfig.http.prefix shouldEqual "v1"
      appConfig.http.publicUri shouldEqual Uri("http://localhost:8080")

      appConfig.runtime.defaultTimeout shouldEqual 30.seconds

      appConfig.cluster.passivationTimeout shouldEqual 10.seconds
      appConfig.cluster.shards shouldEqual 100
      appConfig.cluster.seeds shouldEqual Some("seed1,seed2,seed3")

      appConfig.persistence.journalPlugin shouldEqual "cassandra-journal"
      appConfig.persistence.snapshotStorePlugin shouldEqual "cassandra-snapshot-store"
      appConfig.persistence.queryJournalPlugin shouldEqual "cassandra-query-journal"

      appConfig.pagination.from.value shouldEqual 0
      appConfig.pagination.size.value shouldEqual 10
      appConfig.pagination.maxSize.value shouldEqual 50

      appConfig.admin.baseUri shouldEqual Uri("http://localhost:8080/admin")

      appConfig.prefixes.coreContext shouldEqual ContextUri("http://localhost:8080/v1/contexts/nexus/core/resource/v0.1.0")
      appConfig.prefixes.standardsContext shouldEqual ContextUri("http://localhost:8080/v1/contexts/nexus/core/standards/v0.1.0")
      appConfig.prefixes.linksContext shouldEqual ContextUri("http://localhost:8080/v1/contexts/nexus/core/links/v0.1.0")
      appConfig.prefixes.searchContext shouldEqual ContextUri("http://localhost:8080/v1/contexts/nexus/core/search/v0.1.0")
      appConfig.prefixes.distributionContext shouldEqual ContextUri("http://localhost:8080/v1/contexts/nexus/core/distribution/v0.1.0")
      appConfig.prefixes.errorContext shouldEqual ContextUri("http://localhost:8080/v1/contexts/nexus/core/error/v0.1.0")
      appConfig.prefixes.coreVocabulary shouldEqual Uri("http://localhost:8080/vocabs/nexus/core/terms/v0.1.0/")
    }
  }
}
