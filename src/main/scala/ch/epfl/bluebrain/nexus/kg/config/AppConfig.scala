package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.Path

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.admin.client.config.AdminConfig
import ch.epfl.bluebrain.nexus.iam.client.IamUri
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._

import scala.concurrent.duration.Duration

final case class AppConfig(description: Description,
                           http: HttpConfig,
                           cluster: ClusterConfig,
                           persistence: PersistenceConfig,
                           attachments: AttachmentsConfig,
                           admin: AdminConfig,
                           iam: IamConfig)

object AppConfig {

  final case class Description(name: String, environment: String) {
    def version: String = s"$name-${BuildInfo.version.replaceAll("\\.", "-")}-$environment"

  }

  final case class HttpConfig(interface: String, port: Int, prefix: String, publicUri: Uri)

  final case class ClusterConfig(passivationTimeout: Duration, shards: Int, seeds: Option[String])

  final case class PersistenceConfig(journalPlugin: String, snapshotStorePlugin: String, queryJournalPlugin: String)

  final case class AttachmentsConfig(volume: Path, digestAlgorithm: String)

  final case class IamConfig(baseUri: Uri)

  implicit def configToIamUri(implicit config: AppConfig): IamUri = IamUri(config.iam.baseUri)

}
