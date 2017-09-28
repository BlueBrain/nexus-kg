package ch.epfl.bluebrain.nexus.kg.service

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives.{handleRejections, _}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidator
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains
import ch.epfl.bluebrain.nexus.kg.core.instances.Instances
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.AttachmentLocation
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaImportResolver, Schemas}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.BootstrapRoutes._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.directives.PrefixDirectives.uriPrefix
import ch.epfl.bluebrain.nexus.kg.service.instances.attachments.{AkkaInOutFileStream, RelativeAttachmentLocation}
import ch.epfl.bluebrain.nexus.kg.service.routes._
import ch.epfl.bluebrain.nexus.sourcing.akka.{ShardingAggregate, SourcingAkkaSettings}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.{cors, corsRejectionHandler}
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
  * Construct the service routes
  *
  * @param settings the application settings
  * @param as       the implicitly available [[ActorSystem]]
  * @param ec       the implicitly available [[ExecutionContextExecutor]]
  * @param mt       the implicitly available [[ActorMaterializer]]
  */
class BootstrapRoutes(settings: Settings)(implicit as: ActorSystem, ec: ExecutionContextExecutor, mt: ActorMaterializer) extends BootstrapQuerySettings(settings) {

  private val baseUri = settings.Http.PublicUri

  val apiUri: Uri = if (settings.Http.Prefix.trim.isEmpty) baseUri else baseUri.copy(path = baseUri.path / settings.Http.Prefix)

  private implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient

  val sparqlClient = SparqlClient[Future](settings.Sparql.BaseUri)

  val (orgs, doms, schemas, instances) = operations()

  private val apis = uriPrefix(apiUri) {
    OrganizationRoutes(orgs, sparqlClient, orgSettings, apiUri).routes ~
      DomainRoutes(doms, sparqlClient, domainSettings, apiUri).routes ~
      SchemaRoutes(schemas, sparqlClient, schemaSettings, apiUri).routes ~
      InstanceRoutes(instances, sparqlClient, instanceSettings, apiUri).routes
  }
  private val static = uriPrefix(baseUri)(StaticRoutes().routes)

  private val corsSettings = CorsSettings.defaultSettings.copy(
    allowedMethods = List(GET, PUT, POST, DELETE, OPTIONS, HEAD),
    exposedHeaders = List(Location.name))

  val routes: Route = handleRejections(corsRejectionHandler) {
    cors(corsSettings)(static ~ apis)
  }

  def operations() = {
    implicit val al: AttachmentLocation[Future] = RelativeAttachmentLocation(settings.Attachment.VolumePath)

    val sourcingSettings = SourcingAkkaSettings(journalPluginId = settings.Persistence.QueryJournalPlugin)

    val orgsAgg = ShardingAggregate("organization", sourcingSettings.copy(passivationTimeout = settings.Organizations.PassivationTimeout))(
      Organizations.initial,
      Organizations.next,
      Organizations.eval)

    val inFileProcessor = AkkaInOutFileStream(settings)

    val domsAgg = ShardingAggregate("domain", sourcingSettings.copy(passivationTimeout = settings.Domains.PassivationTimeout))(
      Domains.initial,
      Domains.next,
      Domains.eval)

    val schemasAgg = ShardingAggregate("schema", sourcingSettings.copy(passivationTimeout = settings.Schemas.PassivationTimeout))(
      Schemas.initial,
      Schemas.next,
      Schemas.eval)

    val instancesAgg = ShardingAggregate("instance", sourcingSettings.copy(passivationTimeout = settings.Instances.PassivationTimeout))(
      Instances.initial,
      Instances.next,
      Instances.eval)

    val orgs = Organizations(orgsAgg)
    val doms = Domains(domsAgg, orgs)
    val schemas = Schemas(schemasAgg, doms, apiUri.toString())
    val validator = ShaclValidator[Future](SchemaImportResolver(apiUri.toString(), schemas.fetch))
    implicit val instances = Instances(instancesAgg, schemas, validator, inFileProcessor)
    (orgs, doms, schemas, instances)
  }
}

object BootstrapRoutes {

  /**
    * Constructs all the needed query settings for the service to start.
    *
    * @param settings the application settings
    */
  final def apply(settings: Settings)(implicit as: ActorSystem, ec: ExecutionContextExecutor, mt: ActorMaterializer): BootstrapRoutes = new BootstrapRoutes(settings)

  abstract class BootstrapQuerySettings(settings: Settings) {

    val domainSettings = QuerySettings(
      Pagination(settings.Sparql.From, settings.Sparql.Size),
      settings.Sparql.Domains.Index, settings.Prefixes.CoreVocabulary)

    val orgSettings = QuerySettings(
      Pagination(settings.Sparql.From, settings.Sparql.Size),
      settings.Sparql.Organizations.Index, settings.Prefixes.CoreVocabulary)
    val schemaSettings = QuerySettings(
      Pagination(settings.Sparql.From, settings.Sparql.Size),
      settings.Sparql.Schemas.Index, settings.Prefixes.CoreVocabulary)

    val instanceSettings = QuerySettings(
      Pagination(settings.Sparql.From, settings.Sparql.Size),
      settings.Sparql.Instances.Index, settings.Prefixes.CoreVocabulary)

    implicit val filteringSettings: FilteringSettings = FilteringSettings(
      settings.Prefixes.CoreVocabulary,
      settings.Prefixes.SearchVocabulary)
  }
}