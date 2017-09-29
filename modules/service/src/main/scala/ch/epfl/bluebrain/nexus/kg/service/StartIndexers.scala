package ch.epfl.bluebrain.nexus.kg.service

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
import ch.epfl.bluebrain.nexus.service.commons.persistence.SequentialIndexer

import scala.concurrent.{ExecutionContextExecutor, Future}

object StartIndexers {
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
  final def apply(settings: Settings, sparqlClient: SparqlClient[Future], apiUri: Uri)(implicit as: ActorSystem, ec: ExecutionContextExecutor) = {
    val instanceIndexingSettings = InstanceIndexingSettings(
      settings.Sparql.Instances.Index,
      apiUri,
      settings.Sparql.Instances.GraphBaseNamespace,
      settings.Prefixes.CoreVocabulary)

    SequentialIndexer.start[InstanceEvent](
      InstanceIndexer[Future](sparqlClient, instanceIndexingSettings).apply,
      "instances-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "instance",
      "sequential-instance-indexer")

    val schemaIndexingSettings = SchemaIndexingSettings(
      settings.Sparql.Schemas.Index,
      apiUri,
      settings.Sparql.Schemas.GraphBaseNamespace,
      settings.Prefixes.CoreVocabulary)

    SequentialIndexer.start[SchemaEvent](
      SchemaIndexer[Future](sparqlClient, schemaIndexingSettings).apply,
      "schemas-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "schema",
      "sequential-schema-indexer")

    val domainIndexingSettings = DomainIndexingSettings(
      settings.Sparql.Domains.Index,
      apiUri,
      settings.Sparql.Domains.GraphBaseNamespace,
      settings.Prefixes.CoreVocabulary)

    SequentialIndexer.start[DomainEvent](
      DomainIndexer[Future](sparqlClient, domainIndexingSettings).apply,
      "domains-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "domain",
      "sequential-domain-indexer")

    val orgIndexingSettings = OrganizationIndexingSettings(
      settings.Sparql.Organizations.Index,
      apiUri,
      settings.Sparql.Organizations.GraphBaseNamespace,
      settings.Prefixes.CoreVocabulary)

    SequentialIndexer.start[OrgEvent](
      OrganizationIndexer[Future](sparqlClient, orgIndexingSettings).apply,
      "organization-to-3s",
      settings.Persistence.QueryJournalPlugin,
      "organization",
      "sequential-organization-indexer")
  }
}