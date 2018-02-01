package ch.epfl.bluebrain.nexus.kg.service

import java.time.Clock
import java.util.regex.Pattern

import akka.actor.{ActorSystem, AddressFromURIString}
import akka.cluster.Cluster
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, IOResult, Materializer}
import akka.util.ByteString
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticQueryClient}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.iam.acls.FullAccessControlList
import ch.epfl.bluebrain.nexus.commons.iam.auth.User
import ch.epfl.bluebrain.nexus.commons.iam.io.serialization.JsonLdSerialization
import ch.epfl.bluebrain.nexus.commons.iam.{IamClient, IamUri}
import ch.epfl.bluebrain.nexus.commons.service.directives.PrefixDirectives.uriPrefix
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidator
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test.Resources._
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains
import ch.epfl.bluebrain.nexus.kg.core.instances.Instances
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.AttachmentLocation
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaImportResolver, Schemas}
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.BootstrapService._
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.instances.attachments.{AkkaInOutFileStream, RelativeAttachmentLocation}
import ch.epfl.bluebrain.nexus.kg.service.routes._
import ch.epfl.bluebrain.nexus.sourcing.akka.{ShardingAggregate, SourcingAkkaSettings}
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.{cors, corsRejectionHandler}
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
  * Construct the service routes, operations and cluster.
  *
  * @param settings the application settings
  * @param as       the implicitly available [[ActorSystem]]
  * @param ec       the implicitly available [[ExecutionContextExecutor]]
  * @param mt       the implicitly available [[ActorMaterializer]]
  */
class BootstrapService(settings: Settings)(implicit as: ActorSystem,
                                           ec: ExecutionContextExecutor,
                                           mt: ActorMaterializer,
                                           cl: UntypedHttpClient[Future])
    extends BootstrapQuerySettings(settings) {

  private val baseUri = settings.Http.PublicUri

  // $COVERAGE-OFF$
  override val apiUri: Uri =
    if (settings.Http.Prefix.trim.isEmpty) baseUri
    else baseUri.copy(path = baseUri.path / settings.Http.Prefix)
  // $COVERAGE-ON$

  val sparqlClient = SparqlClient[Future](settings.Sparql.BaseUri)

  val elasticQueryClient = ElasticQueryClient[Future](settings.Elastic.BaseUri)

  val elasticClient = ElasticClient[Future](settings.Elastic.BaseUri, elasticQueryClient)

  val (orgs, doms, schemas, contexts, instances) = operations()

  private implicit val iamC: IamClient[Future]  = iamClient(settings.IAM.BaseUri)
  private implicit val clock: Clock             = Clock.systemUTC
  private implicit val orderedKeys: OrderedKeys = kgOrderedKeys
  private implicit val prefixes: PrefixUris     = settings.Prefixes

  private val apis = uriPrefix(apiUri) {
    implicit val ctxs = contexts
    OrganizationRoutes(orgs, sparqlClient, querySettings, apiUri).routes ~
      DomainRoutes(doms, sparqlClient, querySettings, apiUri).routes ~
      SchemaRoutes(schemas, sparqlClient, querySettings, apiUri).routes ~
      ContextRoutes(sparqlClient, querySettings, apiUri).routes ~
      InstanceRoutes(instances, sparqlClient, querySettings, apiUri).routes
  }
  private val static = uriPrefix(baseUri)(StaticRoutes().routes)

  private val corsSettings = CorsSettings.defaultSettings
    .copy(allowedMethods = List(GET, PUT, POST, DELETE, OPTIONS, HEAD), exposedHeaders = List(Location.name))

  val routes: Route = handleRejections(corsRejectionHandler) {
    cors(corsSettings)(static ~ apis)
  }
  // $COVERAGE-OFF$
  val cluster = Cluster(as)
  private val provided = settings.Cluster.Seeds
    .map(addr => AddressFromURIString(s"akka.tcp://${settings.Description.ActorSystemName}@$addr"))
  private val seeds =
    if (provided.isEmpty) Set(cluster.selfAddress) else provided
  // $COVERAGE-ON$

  private def operations() = {
    implicit val al: AttachmentLocation[Future] = RelativeAttachmentLocation(settings.Attachment.VolumePath)

    val sourcingSettings = SourcingAkkaSettings(journalPluginId = settings.Persistence.QueryJournalPlugin)

    val orgsAgg =
      ShardingAggregate("organization",
                        sourcingSettings.copy(passivationTimeout = settings.Organizations.PassivationTimeout))(
        Organizations.initial,
        Organizations.next,
        Organizations.eval)

    val inFileProcessor = AkkaInOutFileStream(settings)

    val domsAgg = ShardingAggregate(
      "domain",
      sourcingSettings
        .copy(passivationTimeout = settings.Domains.PassivationTimeout))(Domains.initial, Domains.next, Domains.eval)

    val schemasAgg = ShardingAggregate(
      "schema",
      sourcingSettings
        .copy(passivationTimeout = settings.Schemas.PassivationTimeout))(Schemas.initial, Schemas.next, Schemas.eval)

    val ctxAgg = ShardingAggregate(
      "context",
      sourcingSettings
        .copy(passivationTimeout = settings.Schemas.PassivationTimeout))(Contexts.initial, Contexts.next, Contexts.eval)

    val instancesAgg =
      ShardingAggregate("instance", sourcingSettings.copy(passivationTimeout = settings.Instances.PassivationTimeout))(
        Instances.initial,
        Instances.next,
        Instances.eval)

    val orgs      = Organizations(orgsAgg)
    val doms      = Domains(domsAgg, orgs)
    val contexts  = Contexts(ctxAgg, doms, apiUri.toString())
    val schemas   = Schemas(schemasAgg, doms, contexts, apiUri.toString())
    val validator = ShaclValidator[Future](SchemaImportResolver(apiUri.toString(), schemas.fetch))
    implicit val instances: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]] =
      Instances(instancesAgg, schemas, contexts, validator, inFileProcessor)
    (orgs, doms, schemas, contexts, instances)
  }

  def joinCluster(): Unit = cluster.joinSeedNodes(seeds.toList)

  def leaveCluster(): Unit = cluster.leave(cluster.selfAddress)
}

object BootstrapService {

  /**
    * Constructs all the needed query settings for the service to start.
    *
    * @param settings the application settings
    */
  final def apply(settings: Settings)(implicit as: ActorSystem,
                                      ec: ExecutionContextExecutor,
                                      mt: ActorMaterializer,
                                      cl: UntypedHttpClient[Future]): BootstrapService =
    new BootstrapService(settings)

  /**
    * Constructs [[IamClient]] from the provided ''baseIamUri'' and the implicitly available instances
    *
    * @param baseIamUri the baseUri for IAM service
    */
  def iamClient(baseIamUri: Uri)(implicit ec: ExecutionContext,
                                 mt: Materializer,
                                 cl: UntypedHttpClient[Future]): IamClient[Future] = {
    import _root_.io.circe.generic.extras.Configuration
    import _root_.io.circe.generic.extras.auto._
    implicit val identityDecoder = JsonLdSerialization.identityDecoder
    implicit val iamUri          = IamUri(baseIamUri)
    implicit val config          = Configuration.default.withDiscriminator("@type")
    implicit val aclCl           = HttpClient.withAkkaUnmarshaller[FullAccessControlList]
    implicit val userCl          = HttpClient.withAkkaUnmarshaller[User]
    IamClient()
  }

  def kgOrderedKeys: OrderedKeys =
    OrderedKeys(
      List(
        "@context",
        "@id",
        "@type",
        "total",
        "",
        "nxv:rev",
        "nxv:published",
        "nxv:deprecated",
        "rev",
        "published",
        "deprecated",
        "links"
      ))

  abstract class BootstrapQuerySettings(settings: Settings) {

    def apiUri: Uri

    lazy val querySettings = QuerySettings(
      Pagination(settings.Pagination.From, settings.Pagination.Size),
      settings.Pagination.MaxSize,
      settings.Sparql.Index,
      settings.Prefixes.CoreVocabulary,
      apiUri,
      settings.Sparql.Acls.GraphBaseNamespace
    )

    private[service] implicit val filteringSettings: FilteringSettings =
      FilteringSettings(
        settings.Prefixes.CoreVocabulary,
        jsonContentOf("/schemas/nexus/core/search/search_expanded.json",
                      Map(Pattern.quote("{{vocab}}") -> settings.Prefixes.CoreVocabulary.toString()))
      )

  }

}
