package ch.epfl.bluebrain.nexus.kg.service

import java.util.Properties

import _root_.io.circe.generic.extras.Configuration
import _root_.io.circe.generic.extras.auto._
import _root_.io.circe.java8.time._
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.service.persistence.SequentialTagIndexer
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextEvent, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent
import ch.epfl.bluebrain.nexus.kg.indexing.contexts.{ContextSparqlIndexer, ContextSparqlIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.domains._
import ch.epfl.bluebrain.nexus.kg.indexing.instances._
import ch.epfl.bluebrain.nexus.kg.indexing.organizations._
import ch.epfl.bluebrain.nexus.kg.indexing.schemas._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Triggers the start of the indexing process from the resumable projection for all the tags available on the service:
  * instance, schema, domain, organization.
  *
  * @param settings     the app settings
  * @param blazegraphClient the Blazegraph client implementation
  * @param contexts     the context operation bundle
  * @param apiUri       the service public uri + prefix
  * @param as           the implicitly available [[ActorSystem]]
  * @param ec           the implicitly available [[ExecutionContext]]
  */
// $COVERAGE-OFF$
class StartSparqlIndexers(settings: Settings,
                          blazegraphClient: BlazegraphClient[Future],
                          contexts: Contexts[Future],
                          apiUri: Uri)(implicit
                                       as: ActorSystem,
                                       ec: ExecutionContext) {

  private implicit val config: Configuration =
    Configuration.default.withDiscriminator("type")

  startIndexingOrgs()
  startIndexingDomains()
  startIndexingContexts()
  startIndexingSchemas()
  startIndexingInstances()

  private lazy val properties: Map[String, String] = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/index.properties"))
    props.asScala.toMap
  }

  private def initFunction: () => Future[Unit] =
    () =>
      blazegraphClient.namespaceExists.flatMap {
        case true  => Future.successful(())
        case false => blazegraphClient.createNamespace(properties)
    }

  private def startIndexingInstances() = {
    val instanceIndexingSettings = InstanceSparqlIndexingSettings(apiUri,
                                                                  settings.Sparql.Instances.GraphBaseNamespace,
                                                                  settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[InstanceEvent](
      InstanceSparqlIndexer[Future](blazegraphClient, contexts, instanceIndexingSettings).apply _,
      "instances-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "instance",
      "sequential-instance-indexer"
    )
  }

  private def startIndexingSchemas() = {
    val schemaIndexingSettings =
      SchemaSparqlIndexingSettings(apiUri, settings.Sparql.Schemas.GraphBaseNamespace, settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[SchemaEvent](
      SchemaSparqlIndexer[Future](blazegraphClient, contexts, schemaIndexingSettings).apply _,
      "schemas-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "schema",
      "sequential-schema-indexer"
    )
  }

  private def startIndexingContexts() = {
    val contextsIndexingSettings = ContextSparqlIndexingSettings(apiUri,
                                                                 settings.Sparql.Contexts.GraphBaseNamespace,
                                                                 settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[ContextEvent](
      ContextSparqlIndexer[Future](blazegraphClient, contexts, contextsIndexingSettings).apply _,
      "contexts-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "context",
      "sequential-context-indexer"
    )
  }

  private def startIndexingDomains() = {
    val domainIndexingSettings =
      DomainSparqlIndexingSettings(apiUri, settings.Sparql.Domains.GraphBaseNamespace, settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[DomainEvent](
      DomainSparqlIndexer[Future](blazegraphClient, domainIndexingSettings).apply _,
      "domains-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "domain",
      "sequential-domain-indexer"
    )
  }

  private def startIndexingOrgs() = {
    val orgIndexingSettings = OrganizationSparqlIndexingSettings(apiUri,
                                                                 settings.Sparql.Organizations.GraphBaseNamespace,
                                                                 settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[OrgEvent](
      initFunction,
      OrganizationSparqlIndexer[Future](blazegraphClient, contexts, orgIndexingSettings).apply _,
      "organization-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "organization",
      "sequential-organization-indexer"
    )
  }
}
// $COVERAGE-ON$

object StartSparqlIndexers {

  /**
    * Constructs a StartElasticIndexers
    *
    * @param settings     the app settings
    * @param blazegraphClient the Blazegraph client implementation
    * @param apiUri       the service public uri + prefix
    */
  final def apply(settings: Settings,
                  blazegraphClient: BlazegraphClient[Future],
                  contexts: Contexts[Future],
                  apiUri: Uri)(implicit
                               as: ActorSystem,
                               ec: ExecutionContext): StartSparqlIndexers =
    new StartSparqlIndexers(settings, blazegraphClient, contexts, apiUri)

}
// $COVERAGE-ON$
