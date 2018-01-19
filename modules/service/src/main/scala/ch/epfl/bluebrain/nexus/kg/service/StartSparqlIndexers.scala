package ch.epfl.bluebrain.nexus.kg.service

import java.util.Properties

import _root_.io.circe.generic.extras.Configuration
import _root_.io.circe.generic.extras.auto._
import _root_.io.circe.java8.time._
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.kafka.ConsumerSettings
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.iam.acls.{Event => AclEvent}
import ch.epfl.bluebrain.nexus.commons.iam.io.serialization.JsonLdSerialization
import ch.epfl.bluebrain.nexus.commons.service.persistence.SequentialTagIndexer
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextEvent, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent
import ch.epfl.bluebrain.nexus.kg.indexing.acls.{AclIndexer, AclIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.contexts.{ContextSparqlIndexer, ContextSparqlIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.domains._
import ch.epfl.bluebrain.nexus.kg.indexing.instances._
import ch.epfl.bluebrain.nexus.kg.indexing.organizations._
import ch.epfl.bluebrain.nexus.kg.indexing.schemas._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.queue.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Triggers the start of the indexing process from the resumable projection for all the tags available on the service:
  * instance, schema, domain, organization.
  *
  * @param settings     the app settings
  * @param sparqlClient the SPARQL client implementation
  * @param contexts     the context operation bundle
  * @param apiUri       the service public uri + prefix
  * @param as           the implicitly available [[ActorSystem]]
  * @param ec           the implicitly available [[ExecutionContext]]
  */
class StartSparqlIndexers(settings: Settings,
                          sparqlClient: SparqlClient[Future],
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
  startIndexingAcls()

  private lazy val properties: Map[String, String] = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/index.properties"))
    props.asScala.toMap
  }

  private def initFunctionOf(index: String): () => Future[Unit] =
    () =>
      sparqlClient.exists(index).flatMap {
        case true  => Future.successful(())
        case false => sparqlClient.createIndex(index, properties)
    }

  private def startIndexingInstances() = {
    val instanceIndexingSettings = InstanceIndexingSettings(settings.Sparql.Index,
                                                            apiUri,
                                                            settings.Sparql.Instances.GraphBaseNamespace,
                                                            settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[InstanceEvent](
      InstanceIndexer[Future](sparqlClient, contexts, instanceIndexingSettings).apply _,
      "instances-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "instance",
      "sequential-instance-indexer"
    )
  }

  private def startIndexingSchemas() = {
    val schemaIndexingSettings = SchemaIndexingSettings(settings.Sparql.Index,
                                                        apiUri,
                                                        settings.Sparql.Schemas.GraphBaseNamespace,
                                                        settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[SchemaEvent](
      SchemaIndexer[Future](sparqlClient, contexts, schemaIndexingSettings).apply _,
      "schemas-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "schema",
      "sequential-schema-indexer"
    )
  }

  private def startIndexingContexts() = {
    val contextsIndexingSettings = ContextSparqlIndexingSettings(settings.Sparql.Index,
                                                                 apiUri,
                                                                 settings.Sparql.Contexts.GraphBaseNamespace,
                                                                 settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[ContextEvent](
      ContextSparqlIndexer[Future](sparqlClient, contextsIndexingSettings).apply _,
      "contexts-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "context",
      "sequential-context-indexer"
    )
  }

  private def startIndexingDomains() = {
    val domainIndexingSettings = DomainSparqlIndexingSettings(settings.Sparql.Index,
                                                              apiUri,
                                                              settings.Sparql.Domains.GraphBaseNamespace,
                                                              settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[DomainEvent](
      DomainSparqlIndexer[Future](sparqlClient, domainIndexingSettings).apply _,
      "domains-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "domain",
      "sequential-domain-indexer"
    )
  }

  private def startIndexingOrgs() = {
    val orgIndexingSettings = OrganizationSparqlIndexingSettings(settings.Sparql.Index,
                                                                 apiUri,
                                                                 settings.Sparql.Organizations.GraphBaseNamespace,
                                                                 settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[OrgEvent](
      initFunctionOf(settings.Sparql.Index),
      OrganizationSparqlIndexer[Future](sparqlClient, contexts, orgIndexingSettings).apply _,
      "organization-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "organization",
      "sequential-organization-indexer"
    )
  }

  private def startIndexingAcls(): Unit = {

    val aclIndexingSettings = AclIndexingSettings(settings.Sparql.Index,
                                                  apiUri,
                                                  settings.Sparql.Acls.GraphBaseNamespace,
                                                  settings.Prefixes.CoreVocabulary)

    val consumerSettings = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)

    val _ = KafkaConsumer.start[AclEvent](consumerSettings,
                                          AclIndexer[Future](sparqlClient, aclIndexingSettings).apply,
                                          settings.Kafka.Topic,
                                          JsonLdSerialization.eventDecoder)
  }

}

object StartSparqlIndexers {

  // $COVERAGE-OFF$
  /**
    * Constructs a StartElasticIndexers
    *
    * @param settings     the app settings
    * @param sparqlClient the SPARQL client implementation
    * @param apiUri       the service public uri + prefix
    */
  final def apply(settings: Settings, sparqlClient: SparqlClient[Future], contexts: Contexts[Future], apiUri: Uri)(
      implicit
      as: ActorSystem,
      ec: ExecutionContext): StartSparqlIndexers =
    new StartSparqlIndexers(settings, sparqlClient, contexts, apiUri)

  // $COVERAGE-ON$
}
