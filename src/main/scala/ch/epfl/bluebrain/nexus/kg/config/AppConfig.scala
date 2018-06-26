package ch.epfl.bluebrain.nexus.kg.config

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.admin.client.config.AdminConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

import scala.concurrent.duration.Duration

/**
  * Application
  *
  * @param description  service description
  * @param http         http interface configuration
  * @param cluster      akka cluster configuration
  * @param persistence  persistence configuration
  * @param attachments  attachments configuration
  * @param admin        admin client configuration
  * @param iam          IAM client configuration
  */
final case class AppConfig(description: Description,
                           http: HttpConfig,
                           cluster: ClusterConfig,
                           persistence: PersistenceConfig,
                           attachments: AttachmentsConfig,
                           admin: AdminConfig,
                           iam: IamConfig)

object AppConfig {

  /**
    * Service description
    * @param name         service name
    * @param environment  environment in which service is running
    */
  final case class Description(name: String, environment: String) {

    /**
      * Returns the version of the service
      * @return version of the service
      */
    def version: String = s"$name-${BuildInfo.version.replaceAll("\\W", "-")}-$environment"

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
    *  Cluster configuration
    * @param passivationTimeout actor passivation timeout
    * @param shards             number of shards in the cluster
    * @param seeds              seed nodes in the cluster
    */
  final case class ClusterConfig(passivationTimeout: Duration, shards: Int, seeds: Option[String])

  /**
    * Persistence configuration
    * @param journalPlugin        plugin for storing events
    * @param snapshotStorePlugin  plugin for storing snapshots
    * @param queryJournalPlugin   plugin for querying journal events
    */
  final case class PersistenceConfig(journalPlugin: String, snapshotStorePlugin: String, queryJournalPlugin: String)

  /**
    * Attachments configuration
    *
    * @param volume          the base Iri where the attachments are stored
    * @param digestAlgorithm algorithm for checksum calculation
    */
  final case class AttachmentsConfig(volume: AbsoluteIri, digestAlgorithm: String)

  /**
    * IAM config
    * @param baseUri base URI of IAM service
    */
  final case class IamConfig(baseUri: Uri)

}
