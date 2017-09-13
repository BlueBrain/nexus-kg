package ch.epfl.bluebrain.nexus.kg.service

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidator
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainEvent, Domains}
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.AttachmentLocation
import ch.epfl.bluebrain.nexus.kg.core.instances.{InstanceEvent, Instances}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgEvent, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaEvent, SchemaImportResolver, Schemas}
import ch.epfl.bluebrain.nexus.kg.indexing.domains.{DomainIndexer, DomainIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.instances.{InstanceIndexer, InstanceIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.organizations.{OrganizationIndexer, OrganizationIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.indexing.schemas.{SchemaIndexer, SchemaIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.directives.PrefixDirectives._
import ch.epfl.bluebrain.nexus.kg.service.instances.attachments.{AkkaInOutFileStream, RelativeAttachmentLocation}
import ch.epfl.bluebrain.nexus.kg.service.routes._
import ch.epfl.bluebrain.nexus.service.commons.persistence.SequentialIndexer
import ch.epfl.bluebrain.nexus.sourcing.akka.{ShardingAggregate, SourcingAkkaSettings}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import com.typesafe.config.ConfigFactory
import kamon.Kamon

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

//noinspection TypeAnnotation
// $COVERAGE-OFF$
object Main {

  @SuppressWarnings(Array("UnusedMethodParameter"))
  def main(args: Array[String]): Unit = {
    Kamon.start()
    val config = ConfigFactory.load()
    val settings = new Settings(config)

    implicit val as = ActorSystem(settings.Description.ActorSystemName, config)
    implicit val ec = as.dispatcher
    implicit val mt = ActorMaterializer()
    implicit val cl = HttpClient.akkaHttpClient

    val sparqlClient = SparqlClient[Future](settings.Sparql.BaseUri)

    val logger = Logging(as, getClass)
    val sourcingSettings = SourcingAkkaSettings(journalPluginId = settings.Persistence.QueryJournalPlugin)
    val corsSettings = CorsSettings.defaultSettings.copy(
      allowedMethods = List(GET, PUT, POST, DELETE, OPTIONS, HEAD),
      exposedHeaders = List(Location.name))

    val baseUri = settings.Http.PublicUri
    val apiUri  = baseUri.copy(path = baseUri.path / settings.Http.Prefix)

    val cluster = Cluster(as)

    implicit val al: AttachmentLocation[Future] = RelativeAttachmentLocation(settings.Attachment.VolumePath)

    cluster.registerOnMemberUp {
      logger.info("==== Cluster is Live ====")

      val orgsAgg = ShardingAggregate("organization", sourcingSettings.copy(passivationTimeout = settings.Organizations.PassivationTimeout))(
        Organizations.initial,
        Organizations.next,
        Organizations.eval)

      val inFileProcessor = AkkaInOutFileStream(settings)

      val orgs = Organizations(orgsAgg)

      val domsAgg = ShardingAggregate("domain", sourcingSettings.copy(passivationTimeout = settings.Domains.PassivationTimeout))(
        Domains.initial,
        Domains.next,
        Domains.eval)
      val doms = Domains(domsAgg, orgs)

      val schemasAgg = ShardingAggregate("schema", sourcingSettings.copy(passivationTimeout = settings.Schemas.PassivationTimeout))(
        Schemas.initial,
        Schemas.next,
        Schemas.eval)
      val schemas = Schemas(schemasAgg, doms, apiUri.toString())

      val instancesAgg = ShardingAggregate("instance", sourcingSettings.copy(passivationTimeout = settings.Instances.PassivationTimeout))(
        Instances.initial,
        Instances.next,
        Instances.eval)
      val validator = ShaclValidator[Future](SchemaImportResolver(apiUri.toString(), schemas.fetch))
      implicit val instances = Instances(instancesAgg, schemas, validator, inFileProcessor)

      val schemaQuerySettings = QuerySettings(
        Pagination(settings.Sparql.From, settings.Sparql.Size),
        settings.Sparql.Schemas.Index,settings.Prefixes.CoreVocabulary)

      val instanceQuerySettings = QuerySettings(
        Pagination(settings.Sparql.From, settings.Sparql.Size),
        settings.Sparql.Instances.Index,settings.Prefixes.CoreVocabulary)

      val domainQuerySettings = QuerySettings(
        Pagination(settings.Sparql.From, settings.Sparql.Size),
        settings.Sparql.Domains.Index,settings.Prefixes.CoreVocabulary)

      val orgQuerySettings = QuerySettings(
        Pagination(settings.Sparql.From, settings.Sparql.Size),
        settings.Sparql.Organizations.Index,settings.Prefixes.CoreVocabulary)

      val httpBinding = {
        val static   = uriPrefix(baseUri)(StaticRoutes().routes)
        val apis     = uriPrefix(apiUri) {
          OrganizationRoutes(orgs, sparqlClient, orgQuerySettings, apiUri).routes ~
            DomainRoutes(doms, sparqlClient, domainQuerySettings, apiUri).routes ~
            SchemaRoutes(schemas, sparqlClient, schemaQuerySettings, apiUri).routes ~
            InstanceRoutes(instances, sparqlClient, instanceQuerySettings, apiUri).routes ~
            SearchRoutes(sparqlClient, apiUri, instanceQuerySettings).routes
        }
        val routes   = handleRejections(corsRejectionHandler) { cors(corsSettings)(static ~ apis) }
        Http().bindAndHandle(routes, settings.Http.Interface, settings.Http.Port)
      }

      httpBinding onComplete {
        case Success(binding) =>
          logger.info(s"Bound to ${binding.localAddress.getHostString}: ${binding.localAddress.getPort}")
        case Failure(th)      =>
          logger.error(th, "Failed to perform an http binding on {}:{}", settings.Http.Interface, settings.Http.Port)
          Await.result(as.terminate(), 10 seconds)
      }

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

    as.registerOnTermination {
      Kamon.shutdown()
      cluster.leave(cluster.selfAddress)
    }

    // attempt to leave the cluster before shutting down
    val _ = sys.addShutdownHook {
      Await.result(as.terminate().map(_ => ()), 10 seconds)
    }
  }
}
// $COVERAGE-ON$