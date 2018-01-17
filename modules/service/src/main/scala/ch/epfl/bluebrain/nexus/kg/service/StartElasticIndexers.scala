package ch.epfl.bluebrain.nexus.kg.service

import _root_.io.circe.generic.extras.Configuration
import _root_.io.circe.generic.extras.auto._
import _root_.io.circe.java8.time._
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.service.persistence.SequentialTagIndexer
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent
import ch.epfl.bluebrain.nexus.kg.indexing.organizations._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings

import scala.concurrent.{ExecutionContext, Future}

/**
  * Triggers the start of the indexing process from the resumable projection for all the tags available on the service:
  * instance, schema, domain, organization.
  *
  * @param settings     the app settings
  * @param elasticClient the ElasticSearch client implementation
  * @param contexts     the context operation bundle
  * @param apiUri       the service public uri + prefix
  * @param as           the implicitly available [[ActorSystem]]
  * @param ec           the implicitly available [[ExecutionContext]]
  */
class StartElasticIndexers(settings: Settings,
                           elasticClient: ElasticClient[Future],
                           contexts: Contexts[Future],
                           apiUri: Uri)(implicit
                                        as: ActorSystem,
                                        ec: ExecutionContext) {

  private implicit val config: Configuration =
    Configuration.default.withDiscriminator("type")

  startIndexingOrgs()

  private def startIndexingOrgs() = {
    val orgIndexingSettings = OrganizationEsIndexingSettings(settings.Elastic.IndexPrefix,
                                                             settings.Elastic.Type,
                                                             apiUri,
                                                             settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[OrgEvent](
      OrganizationEsIndexer[Future](elasticClient, contexts, orgIndexingSettings).apply _,
      "organization-es-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "organization",
      "sequential-organization-es-indexer"
    )
  }
}

object StartElasticIndexers {

  // $COVERAGE-OFF$
  /**
    * Constructs a StartElasticIndexers
    *
    * @param settings      the app settings
    * @param elasticClient the ElasticSearch client implementation
    * @param apiUri        the service public uri + prefix
    */
  final def apply(settings: Settings, elasticClient: ElasticClient[Future], contexts: Contexts[Future], apiUri: Uri)(
      implicit
      as: ActorSystem,
      ec: ExecutionContext): StartElasticIndexers =
    new StartElasticIndexers(settings, elasticClient, contexts, apiUri)

  // $COVERAGE-ON$
}
