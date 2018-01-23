package ch.epfl.bluebrain.nexus.kg.service

import _root_.io.circe.generic.extras.Configuration
import _root_.io.circe.generic.extras.auto._
import _root_.io.circe.java8.time._
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.service.persistence.SequentialTagIndexer
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextEvent, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import ch.epfl.bluebrain.nexus.kg.indexing.contexts.ContextElasticIndexer
import ch.epfl.bluebrain.nexus.kg.indexing.domains.DomainElasticIndexer
import ch.epfl.bluebrain.nexus.kg.indexing.instances.InstanceElasticIndexer
import ch.epfl.bluebrain.nexus.kg.indexing.organizations._
import ch.epfl.bluebrain.nexus.kg.indexing.schemas.SchemaElasticIndexer
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

  private val indexingSettings = ElasticIndexingSettings(settings.Elastic.IndexPrefix,
                                                         settings.Elastic.Type,
                                                         apiUri,
                                                         settings.Prefixes.CoreVocabulary)

  startIndexingOrgs()
  startIndexingDoms()
  startIndexingContexts()
  startIndexingSchemas()
  startIndexingInstances()

  private def startIndexingOrgs() =
    SequentialTagIndexer.start[OrgEvent](
      OrganizationElasticIndexer[Future](elasticClient, contexts, indexingSettings).apply _,
      "organization-to-elastic",
      settings.Persistence.QueryJournalPlugin,
      "organization",
      "sequential-organization-elastic-indexer"
    )

  private def startIndexingDoms() =
    SequentialTagIndexer.start[DomainEvent](
      DomainElasticIndexer[Future](elasticClient, indexingSettings).apply _,
      "domain-to-elastic",
      settings.Persistence.QueryJournalPlugin,
      "domain",
      "sequential-domain-elastic-indexer"
    )

  private def startIndexingContexts() =
    SequentialTagIndexer.start[ContextEvent](
      ContextElasticIndexer[Future](elasticClient, indexingSettings).apply _,
      "context-to-elastic",
      settings.Persistence.QueryJournalPlugin,
      "context",
      "sequential-context-elastic-indexer"
    )

  private def startIndexingSchemas() =
    SequentialTagIndexer.start[SchemaEvent](
      SchemaElasticIndexer[Future](elasticClient, contexts, indexingSettings).apply _,
      "schema-to-elastic",
      settings.Persistence.QueryJournalPlugin,
      "schema",
      "sequential-schema-elastic-indexer"
    )

  private def startIndexingInstances() =
    SequentialTagIndexer.start[InstanceEvent](
      InstanceElasticIndexer[Future](elasticClient, contexts, indexingSettings).apply _,
      "instance-to-elastic",
      settings.Persistence.QueryJournalPlugin,
      "instance",
      "sequential-instance-elastic-indexer"
    )
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
