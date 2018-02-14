package ch.epfl.bluebrain.nexus.kg.service.config

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig._

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Case class which aggregates the configuration parameters
  *
  * @param description the service description namespace
  * @param instance the service instance specific settings
  * @param http the HTTP binding settings
  * @param runtime the service runtime settings
  * @param cluster the cluster specific settings
  * @param persistence the persistence settings
  * @param projects the project specific settings
  * @param schemas the schema specific settings
  * @param instances the instance specific settings
  * @param prefixes the collection of prefixes used throughout the service
  */
final case class AppConfig(description: DescriptionConfig,
                           instance: InstanceConfig,
                           http: HttpConfig,
                           runtime: RuntimeConfig,
                           cluster: ClusterConfig,
                           persistence: PersistenceConfig,
                           projects: ProjectsConfig,
                           schemas: SchemasConfig,
                           instances: InstancesConfig,
                           prefixes: PrefixesConfig,
                           iam: IamConfig)

object AppConfig {

  object Vocabulary {
    val core: Uri = "https://bbp-nexus.epfl.ch/vocabs/nexus/core/terms/v0.1.0/createdAtTime"
  }

  final case class DescriptionConfig(name: String, environment: String) {
    val version: String = BuildInfo.version
    val ActorSystemName = s"$name-${version.replaceAll("\\.", "-")}-$environment"
  }

  final case class InstanceConfig(interface: String)

  final case class HttpConfig(interface: String, port: Int, prefix: String, publicUri: Uri)

  final case class RuntimeConfig(defaultTimeout: FiniteDuration)

  final case class ClusterConfig(passivationTimeout: Duration, shards: Int, seeds: Option[String]) {
    lazy val seedAddresses: Set[String] =
      seeds.map(_.split(",").toSet).getOrElse(Set.empty[String])
  }

  final case class PersistenceConfig(journalPlugin: String, snapshotStorePlugin: String, queryJournalPlugin: String)

  final case class ProjectsConfig(passivationTimeout: Duration)

  final case class SchemasConfig(passivationTimeout: Duration)

  final case class InstancesConfig(passivationTimeout: Duration, attachment: AttachmentConfig)

  final case class AttachmentConfig(volumePath: Path, digestAlgorithm: String)

  final case class PrefixesConfig(coreContext: Uri,
                                  standardsContext: Uri,
                                  linksContext: Uri,
                                  searchContext: Uri,
                                  distributionContext: Uri,
                                  errorContext: Uri)

  final case class IamConfig(baseUri: Uri)

}
