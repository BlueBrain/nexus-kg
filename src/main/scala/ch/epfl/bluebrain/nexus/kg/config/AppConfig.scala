package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.Path

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import ch.epfl.bluebrain.nexus.admin.client.config.AdminClientConfig
import ch.epfl.bluebrain.nexus.commons.cache.KeyValueStoreConfig
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.kamon.directives.TracingDirectives
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.{AuthToken, Permission}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.storage.Crypto
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig.RetryStrategyConfig
import ch.epfl.bluebrain.nexus.sourcing.akka._
import ch.epfl.bluebrain.nexus.sourcing.projections.IndexingConfig
import io.circe.Json
import javax.crypto.SecretKey

import scala.concurrent.duration.FiniteDuration

/**
  * Application
  *
  * @param description   service description
  * @param http          http interface configuration
  * @param cluster       akka cluster configuration
  * @param persistence   persistence configuration
  * @param storage       storages configuration
  * @param admin         admin client configuration
  * @param iam           IAM client configuration
  * @param sparql        Sparql endpoint configuration
  * @param elasticSearch ElasticSearch endpoint configuration
  * @param pagination    Pagination configuration
  * @param keyValueStore Distributed data configuration
  * @param sourcing      Sourcing configuration
  */
final case class AppConfig(
    description: Description,
    http: HttpConfig,
    cluster: ClusterConfig,
    persistence: PersistenceConfig,
    storage: StorageConfig,
    admin: AdminClientConfig,
    iam: IamConfig,
    sparql: SparqlConfig,
    elasticSearch: ElasticSearchConfig,
    pagination: PaginationConfig,
    keyValueStore: StoreConfig,
    sourcing: SourcingConfig,
)

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
  final case class HttpConfig(interface: String, port: Int, prefix: String, publicUri: Uri) {

    /**
      * The base IRI for all resource IDs.
      */
    val baseIri: AbsoluteIri = url"$publicUri/$prefix".value

    val projectsIri: AbsoluteIri = baseIri + "projects"
  }

  /**
    * KeyValueStore configuration.
    *
    * @param askTimeout         the maximum duration to wait for the replicator to reply
    * @param consistencyTimeout the maximum duration to wait for a consistent read or write across the cluster
    * @param retry              the retry strategy configuration
    * @param indexing           the indexing configuration
    */
  final case class StoreConfig(askTimeout: FiniteDuration,
                               consistencyTimeout: FiniteDuration,
                               retry: RetryStrategyConfig,
                               indexing: IndexingConfig) {
    val keyValueStoreConfig: KeyValueStoreConfig = KeyValueStoreConfig(askTimeout, consistencyTimeout, retry)
  }

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
    * Storage configuration for the allowed storages
    *
    * @param disk         the disk storage configuration
    * @param externalDisk the external disk storage configuration
    * @param amazon       the amazon S3 storage configuration
    * @param password     the password used to encrypt credentials at rest
    * @param salt         the associated salt
    */
  final case class StorageConfig(disk: DiskStorageConfig,
                                 externalDisk: ExternalDiskStorageConfig,
                                 amazon: S3StorageConfig,
                                 password: String,
                                 salt: String) {
    val derivedKey: SecretKey = Crypto.deriveKey(password, salt)
  }

  /**
    * Amazon S3 storage configuration
    *
    * @param digestAlgorithm algorithm for checksum calculation
    * @param readPermission  the default permission required in order to download a file from a s3 storage
    * @param writePermission the default permission required in order to upload a file to a s3 storage
    */
  final case class S3StorageConfig(digestAlgorithm: String, readPermission: Permission, writePermission: Permission)

  /**
    * Disk storage configuration
    *
    * @param volume          the base [[Path]] where the files are stored
    * @param digestAlgorithm algorithm for checksum calculation
    * @param readPermission  the default permission required in order to download a file from a disk storage
    * @param writePermission the default permission required in order to upload a file to a disk storage
    */
  final case class DiskStorageConfig(volume: Path,
                                     digestAlgorithm: String,
                                     readPermission: Permission,
                                     writePermission: Permission)

  /**
    * External Disk storage configuration
    *
    * @param defaultEndpoint    the default endpoint of the external disk storage
    * @param defaultCredentials the default credentials for the defaultEnpoint of the external disk storage
    * @param readPermission     the default permission required in order to download a file from a disk storage
    * @param writePermission    the default permission required in order to upload a file to a disk storage
    */
  final case class ExternalDiskStorageConfig(defaultEndpoint: Uri,
                                             defaultCredentials: Option[AuthToken],
                                             digestAlgorithm: String,
                                             readPermission: Permission,
                                             writePermission: Permission)

  /**
    * IAM config
    *
    * @param publicIri           base URL for all the identity IDs, including prefix.
    * @param internalIri         base URL for all the HTTP calls, including prefix.
    * @param serviceAccountToken the service account token to execute calls to IAM
    * @param sseRetryDelay       delay for retrying after completion on SSE
    */
  final case class IamConfig(
      publicIri: AbsoluteIri,
      internalIri: AbsoluteIri,
      serviceAccountToken: Option[AuthToken],
      sseRetryDelay: FiniteDuration,
  ) {
    val iamClient: IamClientConfig = IamClientConfig(publicIri, internalIri, sseRetryDelay)
  }

  /**
    * Collection of configurable settings specific to the Sparql indexer.
    *
    * @param base         the base uri
    * @param indexPrefix  the prefix of the index
    * @param username     the SPARQL endpoint username
    * @param password     the SPARQL endpoint password
    * @param defaultIndex the SPARQL default index
    * @param indexing     the indexing configuration
    * @param query        the query retry strategy configuration
    */
  final case class SparqlConfig(base: Uri,
                                indexPrefix: String,
                                username: Option[String],
                                password: Option[String],
                                defaultIndex: String,
                                indexing: IndexingConfig,
                                query: RetryStrategyConfig) {

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
    * @param indexing     the indexing configuration
    * @param query        the query retry strategy configuration
    */
  final case class ElasticSearchConfig(base: Uri,
                                       indexPrefix: String,
                                       docType: String,
                                       defaultIndex: String,
                                       indexing: IndexingConfig,
                                       query: RetryStrategyConfig)

  /**
    * Pagination configuration
    *
    * @param defaultSize  the default number of results per page
    * @param sizeLimit    the maximum number of results per page
    * @param fromLimit    the maximum value of `from` parameter
    */
  final case class PaginationConfig(defaultSize: Int, sizeLimit: Int, fromLimit: Int)

  val iriResolution: Map[AbsoluteIri, Json] = Map(
    tagCtxUri         -> tagCtx,
    resourceCtxUri    -> resourceCtx,
    shaclCtxUri       -> shaclCtx,
    resolverCtxUri    -> resolverCtx,
    viewCtxUri        -> viewCtx,
    storageCtxUri     -> storageCtx,
    resolverSchemaUri -> resolverSchema,
    viewSchemaUri     -> viewSchema,
    storageSchemaUri  -> storageSchema
  )

  val orderedKeys = OrderedKeys(
    List(
      "@context",
      "@id",
      "@type",
      "code",
      "message",
      "details",
      nxv.resourceId.prefix,
      nxv.organization.prefix,
      nxv.storageId.prefix,
      nxv.total.prefix,
      nxv.maxScore.prefix,
      nxv.results.prefix,
      nxv.score.prefix,
      "",
      nxv.readPermission.prefix,
      nxv.writePermission.prefix,
      nxv.algorithm.prefix,
      nxv.self.prefix,
      nxv.constrainedBy.prefix,
      nxv.project.prefix,
      nxv.projectUuid.prefix,
      nxv.organizationUuid.prefix,
      nxv.rev.prefix,
      nxv.deprecated.prefix,
      nxv.createdAt.prefix,
      nxv.createdBy.prefix,
      nxv.updatedAt.prefix,
      nxv.updatedBy.prefix,
      nxv.instant.prefix,
      nxv.eventSubject.prefix
    ))

  val tracing = new TracingDirectives()

  implicit def toSparql(implicit appConfig: AppConfig): SparqlConfig               = appConfig.sparql
  implicit def toElasticSearch(implicit appConfig: AppConfig): ElasticSearchConfig = appConfig.elasticSearch
  implicit def toPersistence(implicit appConfig: AppConfig): PersistenceConfig     = appConfig.persistence
  implicit def toPagination(implicit appConfig: AppConfig): PaginationConfig       = appConfig.pagination
  implicit def toHttp(implicit appConfig: AppConfig): HttpConfig                   = appConfig.http
  implicit def toIam(implicit appConfig: AppConfig): IamConfig                     = appConfig.iam
  implicit def toIamClient(implicit appConfig: AppConfig): IamClientConfig         = appConfig.iam.iamClient
  implicit def toAdmin(implicit appConfig: AppConfig): AdminClientConfig           = appConfig.admin
  implicit def toSourcing(implicit appConfig: AppConfig): SourcingConfig           = appConfig.sourcing
  implicit def toStore(implicit appConfig: AppConfig): StoreConfig                 = appConfig.keyValueStore
  implicit def toKVS(implicit appConfig: AppConfig): KeyValueStoreConfig           = appConfig.keyValueStore.keyValueStoreConfig
  implicit def toStorage(implicit appConfig: AppConfig): StorageConfig             = appConfig.storage
  implicit def toSecretKey(implicit storageConfig: StorageConfig): SecretKey       = storageConfig.derivedKey

}
