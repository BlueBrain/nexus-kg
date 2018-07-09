package ch.epfl.bluebrain.nexus.kg.core.config

import java.nio.file.Path

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.service.http.{Path => HttpPath}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.{NonNegative, Positive}

import scala.concurrent.duration.{Duration, FiniteDuration}

final case class AppConfig(description: DescriptionConfig,
                           instance: InstanceConfig,
                           http: HttpConfig,
                           runtime: RuntimeConfig,
                           cluster: ClusterConfig,
                           persistence: PersistenceConfig,
                           attachment: AttachmentConfig,
                           pagination: PaginationConfig,
                           prefixes: PrefixesConfig,
                           elastic: ElasticConfig,
                           sparql: SparqlConfig,
                           admin: AdminConfig)

object AppConfig {
  final case class DescriptionConfig(name: String) {
    val version: String = BuildInfo.version
    val actorSystemName = s"$name-${version.replaceAll("\\W", "-")}"
  }

  final case class InstanceConfig(interface: String)

  final case class HttpConfig(interface: String, port: Int, prefix: String, publicUri: Uri)

  final case class RuntimeConfig(defaultTimeout: FiniteDuration)

  final case class ClusterConfig(passivationTimeout: Duration, shards: Int, seeds: Option[String])

  final case class PersistenceConfig(journalPlugin: String, snapshotStorePlugin: String, queryJournalPlugin: String)

  final case class AttachmentConfig(volume: Path, digestAlgorithm: String)

  final case class PaginationConfig(from: Long Refined NonNegative,
                                    size: Int Refined Positive,
                                    maxSize: Int Refined Positive) {
    val pagination: Pagination = Pagination(from.value, size.value)
  }

  final case class PrefixesConfig(coreContext: AbsoluteIri,
                                  standardsContext: AbsoluteIri,
                                  linksContext: AbsoluteIri,
                                  searchContext: AbsoluteIri,
                                  distributionContext: AbsoluteIri,
                                  errorContext: AbsoluteIri,
                                  coreVocabulary: Uri)

  final case class ElasticConfig(baseUri: Uri, indexPrefix: String, docType: String)

  final case class SparqlConfig(baseUri: Uri, endpoint: Uri, index: String)

  final case class AdminConfig(baseUri: Uri, projectsPath: String) {
    val projectUri: Uri = baseUri.append(HttpPath(projectsPath))
  }

  implicit def adminConfigFromConfig(implicit config: AppConfig): AdminConfig     = config.admin
  implicit def attachmentFromConfig(implicit config: AppConfig): AttachmentConfig = config.attachment
}
