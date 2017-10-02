package ch.epfl.bluebrain.nexus.kg.service

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
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
import ch.epfl.bluebrain.nexus.kg.service.StartIndexers._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.service.commons.persistence.SequentialIndexer

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}

/**
  * Triggers the start of the indexing process from the resumable projection for all the tags avaialable on the service:
  * instance, schema, domain, organization.
  *
  * @param settings     the app settings
  * @param sparqlClient the SPARQL client implementation
  * @param apiUri       the service public uri + prefix
  * @param as           the implicitly available [[ActorSystem]]
  * @param ec           the implicitly available [[ExecutionContextExecutor]]
  */
class StartIndexers(settings: Settings, sparqlClient: SparqlClient[Future], apiUri: Uri)(implicit
  as: ActorSystem, ec: ExecutionContextExecutor) extends Actor with ActorLogging {

  val instanceIndexingSettings = InstanceIndexingSettings(
    settings.Sparql.Instances.Index,
    apiUri,
    settings.Sparql.Instances.GraphBaseNamespace,
    settings.Prefixes.CoreVocabulary)

  val schemaIndexingSettings = SchemaIndexingSettings(
    settings.Sparql.Schemas.Index,
    apiUri,
    settings.Sparql.Schemas.GraphBaseNamespace,
    settings.Prefixes.CoreVocabulary)

  val domainIndexingSettings = DomainIndexingSettings(
    settings.Sparql.Domains.Index,
    apiUri,
    settings.Sparql.Domains.GraphBaseNamespace,
    settings.Prefixes.CoreVocabulary)

  val orgIndexingSettings = OrganizationIndexingSettings(
    settings.Sparql.Organizations.Index,
    apiUri,
    settings.Sparql.Organizations.GraphBaseNamespace,
    settings.Prefixes.CoreVocabulary)

  lazy val properties: Map[String, String] = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/index.properties"))
    props.asScala.toMap
  }

  override def preStart(): Unit = {
    super.preStart()
    self ! Start
  }

  override def receive = {
    case Start =>
      log.info("Received start signal. Creating Blazegraph namespaces if they do not exists and start indexing.")
      List(instanceIndexingSettings.index -> startIndexingInstances,
        schemaIndexingSettings.index -> startIndexingSchemas,
        domainIndexingSettings.index -> startIndexingDomains,
        orgIndexingSettings.index -> startIndexingOrgs)
        .foreach { case (index, startFunction) =>
          for {
            exists <- sparqlClient.exists(index)
            _ <- exists match {
              case true  => Future.successful(())
              case false => sparqlClient.createIndex(index, properties)
            }
          } yield (startFunction.apply())
          self ! Stop
        }

    case Stop =>
      log.info("Received stop signal. Stopping CLuster Singleton Actor")
      context.stop(self)
  }

  private def startIndexingInstances = () => {
    SequentialIndexer.start[InstanceEvent](
      InstanceIndexer[Future](sparqlClient, instanceIndexingSettings).apply,
      "instances-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "instance",
      "sequential-instance-indexer")
  }

  private def startIndexingSchemas = () => {
    SequentialIndexer.start[SchemaEvent](
      SchemaIndexer[Future](sparqlClient, schemaIndexingSettings).apply,
      "schemas-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "schema",
      "sequential-schema-indexer")
  }

  private def startIndexingDomains = () => {
    SequentialIndexer.start[DomainEvent](
      DomainIndexer[Future](sparqlClient, domainIndexingSettings).apply,
      "domains-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "domain",
      "sequential-domain-indexer")
  }

  private def startIndexingOrgs = () => {
    SequentialIndexer.start[OrgEvent](
      OrganizationIndexer[Future](sparqlClient, orgIndexingSettings).apply,
      "organization-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "organization",
      "sequential-organization-indexer")
  }

}

object StartIndexers {

  final case object Stop

  final case object Start

  // $COVERAGE-OFF$
  private def props(settings: Settings, sparqlClient: SparqlClient[Future], apiUri: Uri)(implicit
    as: ActorSystem, ec: ExecutionContextExecutor): Props = {
    ClusterSingletonManager.props(
      Props(new StartIndexers(settings, sparqlClient, apiUri)),
      terminationMessage = Stop,
      settings = ClusterSingletonManagerSettings(as))
  }

  /**
    * Constructs the Cluster Singleton actor which deals with the
    * creation of the namespaces for the several indexes
    * and triggers the indexing process afterwards.
    *
    * @param settings     the app settings
    * @param sparqlClient the SPARQL client implementation
    * @param apiUri       the service public uri + prefix
    */
  final def apply(settings: Settings, sparqlClient: SparqlClient[Future], apiUri: Uri)(implicit
    as: ActorSystem, ec: ExecutionContextExecutor): ActorRef =
    as.actorOf(props(settings, sparqlClient, apiUri), "StartIndexersSingleton")

  // $COVERAGE-ON$
}