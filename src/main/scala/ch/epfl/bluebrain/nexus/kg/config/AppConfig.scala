package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.BasicHttpCredentials
import akka.util.Timeout
import cats.ApplicativeError
import cats.effect.Timer
import ch.epfl.bluebrain.nexus.admin.client.config.AdminConfig
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.Backoff
import ch.epfl.bluebrain.nexus.service.kamon.directives.TracingDirectives
import ch.epfl.bluebrain.nexus.sourcing.akka.{RetryStrategy => SourcingRetryStrategy, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

/**
  * Application
  *
  * @param description service description
  * @param http        http interface configuration
  * @param cluster     akka cluster configuration
  * @param persistence persistence configuration
  * @param attachments attachments configuration
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
                           attachments: AttachmentsConfig,
                           admin: AdminConfig,
                           iam: IamConfig,
                           sparql: SparqlConfig,
                           elastic: ElasticConfig,
                           pagination: PaginationConfig,
                           indexing: IndexingConfig,
                           kafka: KafkaConfig,
                           sourcing: SourcingConfig)

object AppConfig {

  final case class PassivationStrategyConfig(
      lapsedSinceLastInteraction: Option[FiniteDuration],
      lapsedSinceRecoveryCompleted: Option[FiniteDuration],
  )

  /**
    * Retry strategy configuration.
    *
    * @param strategy     the type of strategy; possible options are "never", "once" and "exponential"
    * @param initialDelay the initial delay before retrying that will be multiplied with the 'factor' for each attempt
    *                     (applicable only for strategy "exponential")
    * @param maxRetries   maximum number of retries in case of failure (applicable only for strategy "exponential")
    * @param factor       the exponential factor (applicable only for strategy "exponential")
    */
  final case class RetryStrategyConfig(
      strategy: String,
      initialDelay: FiniteDuration,
      maxRetries: Int,
      factor: Int
  ) {

    /**
      * Computes a retry strategy from the provided configuration.
      */
    def retryStrategy[F[_]: Timer, E](implicit F: ApplicativeError[F, E]): SourcingRetryStrategy[F] =
      strategy match {
        case "exponential" =>
          SourcingRetryStrategy.exponentialBackoff(initialDelay, maxRetries, factor)
        case "once" =>
          SourcingRetryStrategy.once
        case _ =>
          SourcingRetryStrategy.never
      }
  }

  /**
    * Sourcing configuration.
    *
    * @param askTimeout                        timeout for the message exchange with the aggregate actor
    * @param queryJournalPlugin                the query (read) plugin journal id
    * @param commandEvaluationTimeout          timeout for evaluating commands
    * @param commandEvaluationExecutionContext the execution context where commands are to be evaluated
    * @param shards                            the number of shards for the aggregate
    * @param passivation                       the passivation strategy configuration
    * @param retry                             the retry strategy configuration
    */
  final case class SourcingConfig(
      askTimeout: FiniteDuration,
      queryJournalPlugin: String,
      commandEvaluationTimeout: FiniteDuration,
      commandEvaluationExecutionContext: String,
      shards: Int,
      passivation: PassivationStrategyConfig,
      retry: RetryStrategyConfig,
  ) {

    /**
      * Computes an [[AkkaSourcingConfig]] using an implicitly available actor system.
      *
      * @param as the underlying actor system
      */
    def akkaSourcingConfig(implicit as: ActorSystem): AkkaSourcingConfig =
      AkkaSourcingConfig(
        askTimeout = Timeout(askTimeout),
        readJournalPluginId = queryJournalPlugin,
        commandEvaluationMaxDuration = commandEvaluationTimeout,
        commandEvaluationExecutionContext =
          if (commandEvaluationExecutionContext == "akka") as.dispatcher
          else ExecutionContext.global
      )

    /**
      * Computes a passivation strategy from the provided configuration and the passivation evaluation function.
      *
      * @param shouldPassivate whether aggregate should passivate after a message exchange
      * @tparam State   the type of the aggregate state
      * @tparam Command the type of the aggregate command
      */
    def passivationStrategy[State, Command](
        shouldPassivate: (String, String, State, Option[Command]) => Boolean =
          (_: String, _: String, _: State, _: Option[Command]) => false
    ): PassivationStrategy[State, Command] =
      PassivationStrategy(
        passivation.lapsedSinceLastInteraction,
        passivation.lapsedSinceRecoveryCompleted,
        shouldPassivate
      )
  }

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
    * Attachments configuration
    *
    * @param volume          the base [[Path]] where the attachments are stored
    * @param digestAlgorithm algorithm for checksum calculation
    */
  final case class AttachmentsConfig(volume: Path, digestAlgorithm: String)

  /**
    * IAM config
    *
    * @param baseUri              base URI of IAM service
    * @param serviceAccountToken  the service account token to execute calls to IAM
    * @param cacheRefreshInterval the maximum tolerated inactivity period after which the cached ACLs will be refreshed
    */
  final case class IamConfig(baseUri: Uri, serviceAccountToken: Option[AuthToken], cacheRefreshInterval: FiniteDuration)

  /**
    * Kafka config
    *
    * @param adminTopic the topic for account and project events
    * @param migration  the v0 events migration config
    */
  final case class KafkaConfig(adminTopic: String, migration: MigrationConfig)

  /**
    * Migration config
    *
    * @param enabled    whether the v0 event migration is enabled
    * @param topic      the Kafka topic to read v0 events from
    * @param baseUri    the base URI for v0 ids
    * @param projectRef the target project reference, where events will be migrated to
    * @param pattern    the regex pattern to select event ids that will be migrated
    */
  final case class MigrationConfig(enabled: Boolean,
                                   topic: String,
                                   baseUri: AbsoluteIri,
                                   projectRef: ProjectRef,
                                   pattern: Regex)

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
  implicit def toAdmin(implicit appConfig: AppConfig): AdminConfig              = appConfig.admin
  implicit def toIndexing(implicit appConfig: AppConfig): IndexingConfig        = appConfig.indexing
  implicit def toSourcingConfing(implicit appConfig: AppConfig): SourcingConfig = appConfig.sourcing

}
