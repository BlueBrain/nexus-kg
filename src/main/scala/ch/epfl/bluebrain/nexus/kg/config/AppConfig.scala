package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.Path

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import ch.epfl.bluebrain.nexus.admin.client.config.AdminConfig
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.Backoff
import ch.epfl.bluebrain.nexus.service.kamon.directives.TracingDirectives
import ch.epfl.bluebrain.nexus.sourcing.akka._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

import scala.concurrent.duration.FiniteDuration

/**
  * Application
  *
  * @param description service description
  * @param http        http interface configuration
  * @param cluster     akka cluster configuration
  * @param persistence persistence configuration
  * @param files    files configuration
  * @param admin       admin client configuration
  * @param iam         IAM client configuration
  * @param sparql      Sparql endpoint configuration
  * @param elastic     ElasticSearch endpoint configuration
  * @param pagination  Pagination configuration
  * @param indexing    Indexing configuration
  */
final case class AppConfig(description: Description,
                           http: HttpConfig,
                           cluster: ClusterConfig,
                           persistence: PersistenceConfig,
                           files: FileConfig,
                           admin: AdminConfig,
                           iam: IamConfig,
                           sparql: SparqlConfig,
                           elastic: ElasticConfig,
                           pagination: PaginationConfig,
                           indexing: IndexingConfig,
                           kafka: KafkaConfig,
                           sourcing: SourcingConfig)

object AppConfig {

  /**
    * Service description
    *
    * @param name service name
    */
  final case class Description(name: String) {

    /**
      * @return the version of the service
      */
    val version: String = BuildInfo.version

    /**
      * @return the full name of the service (name + version)
      */
    val fullName: String = s"$name-${version.replaceAll("\\W", "-")}"

  }

  /**
    * HTTP configuration
    *
    * @param interface  interface to bind to
    * @param port       port to bind to
    * @param prefix     prefix to add to HTTP routes
    * @param publicUri  public URI of the service
    */
  final case class HttpConfig(interface: String, port: Int, prefix: String, publicUri: Uri)

  /**
    * Cluster configuration
    *
    * @param passivationTimeout actor passivation timeout
    * @param replicationTimeout replication / distributed data timeout
    * @param shards             number of shards in the cluster
    * @param seeds              seed nodes in the cluster
    */
  final case class ClusterConfig(passivationTimeout: FiniteDuration,
                                 replicationTimeout: FiniteDuration,
                                 shards: Int,
                                 seeds: Option[String])

  /**
    * Persistence configuration
    *
    * @param journalPlugin        plugin for storing events
    * @param snapshotStorePlugin  plugin for storing snapshots
    * @param queryJournalPlugin   plugin for querying journal events
    */
  final case class PersistenceConfig(journalPlugin: String, snapshotStorePlugin: String, queryJournalPlugin: String)

  /**
    * File configuration
    *
    * @param volume          the base [[Path]] where the files are stored
    * @param digestAlgorithm algorithm for checksum calculation
    */
  final case class FileConfig(volume: Path, digestAlgorithm: String)

  /**
    * IAM config
    *
    * @param baseUri              base URI of IAM service
    * @param serviceAccountToken  the service account token to execute calls to IAM
    * @param cacheRefreshInterval the maximum tolerated inactivity period after which the cached ACLs will be refreshed
    */
  final case class IamConfig(baseUri: Uri,
                             serviceAccountToken: Option[AuthToken],
                             cacheRefreshInterval: FiniteDuration) {
    val iamClient: IamClientConfig = IamClientConfig(url"${baseUri.toString}".value)
  }

  /**
    * Kafka config
    *
    * @param adminTopic the topic for account and project events
    */
  final case class KafkaConfig(adminTopic: String)

  /**
    * Collection of configurable settings specific to the Sparql indexer.
    *
    * @param base         the base uri
    * @param username     the SPARQL endpoint username
    * @param password     the SPARQL endpoint password
    * @param defaultIndex the SPARQL default index
    */
  final case class SparqlConfig(base: Uri, username: Option[String], password: Option[String], defaultIndex: String) {

    val akkaCredentials: Option[BasicHttpCredentials] =
      for {
        user <- username
        pass <- password
      } yield BasicHttpCredentials(user, pass)
  }

  /**
    * Collection of configurable settings specific to the ElasticSearch indexer.
    *
    * @param base         the application base uri for operating on resources
    * @param indexPrefix  the prefix of the index
    * @param docType      the name of the `type`
    * @param defaultIndex the default index
    */
  final case class ElasticConfig(base: Uri, indexPrefix: String, docType: String, defaultIndex: String)

  /**
    * Pagination configuration
    *
    * @param from      the start offset
    * @param size      the default number of results per page
    * @param sizeLimit the maximum number of results per page
    */
  final case class PaginationConfig(from: Long, size: Int, sizeLimit: Int) {
    val pagination: Pagination = Pagination(from, size)
  }

  /**
    * Retry configuration with Exponential backoff
    *
    * @param maxCount     the maximum number of times an index function is retried
    * @param maxDuration  the maximum amount of time to wait between two retries
    * @param randomFactor the jitter added between retries
    */
  final case class Retry(maxCount: Int, maxDuration: FiniteDuration, randomFactor: Double) {
    val strategy: RetryStrategy = Backoff(maxDuration, randomFactor)
  }

  /**
    * Indexing configuration
    *
    * @param batch        the maximum number of events taken on each batch
    * @param batchTimeout the maximum amount of time to wait for the number of events to be taken on each batch
    * @param retry        the retry configuration when indexing failures
    */
  final case class IndexingConfig(batch: Int, batchTimeout: FiniteDuration, retry: Retry)

  val iriResolution = Map(
    tagCtxUri         -> tagCtx,
    resourceCtxUri    -> resourceCtx,
    shaclCtxUri       -> shaclCtx,
    resolverCtxUri    -> resolverCtx,
    viewCtxUri        -> viewCtx,
    resolverSchemaUri -> resolverSchema,
    viewSchemaUri     -> viewSchema
  )

  val orderedKeys = OrderedKeys(
    List(
      "@context",
      "@id",
      "@type",
      "code",
      "message",
      "details",
      nxv.total.prefix,
      nxv.maxScore.prefix,
      nxv.results.prefix,
      nxv.score.prefix,
      "",
      nxv.self.prefix,
      nxv.constrainedBy.prefix,
      nxv.project.prefix,
      nxv.createdAt.prefix,
      nxv.createdBy.prefix,
      nxv.updatedAt.prefix,
      nxv.updatedBy.prefix,
      nxv.rev.prefix,
      nxv.deprecated.prefix
    ))

  val tracing = new TracingDirectives()

  implicit def toSparql(implicit appConfig: AppConfig): SparqlConfig            = appConfig.sparql
  implicit def toElastic(implicit appConfig: AppConfig): ElasticConfig          = appConfig.elastic
  implicit def toPersistence(implicit appConfig: AppConfig): PersistenceConfig  = appConfig.persistence
  implicit def toPagination(implicit appConfig: AppConfig): PaginationConfig    = appConfig.pagination
  implicit def toHttp(implicit appConfig: AppConfig): HttpConfig                = appConfig.http
  implicit def toIam(implicit appConfig: AppConfig): IamConfig                  = appConfig.iam
  implicit def toIamClient(implicit appConfig: AppConfig): IamClientConfig      = appConfig.iam.iamClient
  implicit def toAdmin(implicit appConfig: AppConfig): AdminConfig              = appConfig.admin
  implicit def toIndexing(implicit appConfig: AppConfig): IndexingConfig        = appConfig.indexing
  implicit def toSourcingConfing(implicit appConfig: AppConfig): SourcingConfig = appConfig.sourcing

}
