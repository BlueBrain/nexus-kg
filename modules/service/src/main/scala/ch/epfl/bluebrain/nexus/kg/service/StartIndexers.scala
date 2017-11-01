package ch.epfl.bluebrain.nexus.kg.service

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent
import ch.epfl.bluebrain.nexus.kg.indexing.domains._
import ch.epfl.bluebrain.nexus.kg.indexing.instances._
import ch.epfl.bluebrain.nexus.kg.indexing.organizations._
import ch.epfl.bluebrain.nexus.kg.indexing.schemas._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.commons.service.persistence.SequentialTagIndexer

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import _root_.io.circe.java8.time._
import _root_.io.circe.generic.extras.auto._
import _root_.io.circe.generic.extras.Configuration

/**
  * Triggers the start of the indexing process from the resumable projection for all the tags avaialable on the service:
  * instance, schema, domain, organization.
  *
  * @param settings     the app settings
  * @param sparqlClient the SPARQL client implementation
  * @param apiUri       the service public uri + prefix
  * @param as           the implicitly available [[ActorSystem]]
  * @param ec           the implicitly available [[ExecutionContext]]
  */
class StartIndexers(settings: Settings, sparqlClient: SparqlClient[Future], apiUri: Uri)(implicit
                                                                                         as: ActorSystem,
                                                                                         ec: ExecutionContext) {

  private implicit val config: Configuration =
    Configuration.default.withDiscriminator("type")

  startIndexingOrgs()
  startIndexingDomains()
  startIndexingSchemas()
  startIndexingInstances()

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
    val instanceIndexingSettings = InstanceIndexingSettings(settings.Sparql.Instances.Index,
                                                            apiUri,
                                                            settings.Sparql.Instances.GraphBaseNamespace,
                                                            settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[InstanceEvent](
      initFunctionOf(settings.Sparql.Instances.Index),
      InstanceIndexer[Future](sparqlClient, instanceIndexingSettings).apply _,
      "instances-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "instance",
      "sequential-instance-indexer"
    )
  }

  private def startIndexingSchemas() = {
    val schemaIndexingSettings = SchemaIndexingSettings(settings.Sparql.Schemas.Index,
                                                        apiUri,
                                                        settings.Sparql.Schemas.GraphBaseNamespace,
                                                        settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[SchemaEvent](
      initFunctionOf(settings.Sparql.Schemas.Index),
      SchemaIndexer[Future](sparqlClient, schemaIndexingSettings).apply _,
      "schemas-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "schema",
      "sequential-schema-indexer"
    )
  }

  private def startIndexingDomains() = {
    val domainIndexingSettings = DomainIndexingSettings(settings.Sparql.Domains.Index,
                                                        apiUri,
                                                        settings.Sparql.Domains.GraphBaseNamespace,
                                                        settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[DomainEvent](
      initFunctionOf(settings.Sparql.Domains.Index),
      DomainIndexer[Future](sparqlClient, domainIndexingSettings).apply _,
      "domains-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "domain",
      "sequential-domain-indexer"
    )
  }

  private def startIndexingOrgs() = {
    val orgIndexingSettings = OrganizationIndexingSettings(settings.Sparql.Organizations.Index,
                                                           apiUri,
                                                           settings.Sparql.Organizations.GraphBaseNamespace,
                                                           settings.Prefixes.CoreVocabulary)

    SequentialTagIndexer.start[OrgEvent](
      initFunctionOf(settings.Sparql.Organizations.Index),
      OrganizationIndexer[Future](sparqlClient, orgIndexingSettings).apply _,
      "organization-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "organization",
      "sequential-organization-indexer"
    )
  }

}

object StartIndexers {

  // $COVERAGE-OFF$
  /**
    * Constructs a StartIndexers
    *
    * @param settings     the app settings
    * @param sparqlClient the SPARQL client implementation
    * @param apiUri       the service public uri + prefix
    */
  final def apply(settings: Settings, sparqlClient: SparqlClient[Future], apiUri: Uri)(
      implicit
      as: ActorSystem,
      ec: ExecutionContext): StartIndexers =
    new StartIndexers(settings, sparqlClient, apiUri)

  // $COVERAGE-ON$
}
