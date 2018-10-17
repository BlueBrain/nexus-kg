package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.{Path, Paths}

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.admin.client.config.AdminConfig
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import com.typesafe.config.Config
import pureconfig.ConvertHelpers.catchReadError
import pureconfig._

import scala.util.matching.Regex

/**
  * Akka settings extension to expose application configuration.  It typically uses the configuration instance of the
  * actor system as the configuration root.
  *
  * @param config the configuration instance to read
  */
@SuppressWarnings(Array("LooksLikeInterpolatedString", "OptionGet"))
class Settings(config: Config) extends Extension {

  private implicit val uriConverter: ConfigConvert[Uri] =
    ConfigConvert.viaString[Uri](catchReadError(s => Uri(s)), _.toString)

  private implicit val absoluteIriConverter: ConfigConvert[AbsoluteIri] =
    ConfigConvert.viaStringOpt[AbsoluteIri](s => Iri.absolute(s).toOption, _.asString)

  private implicit val authTokenConverter: ConfigConvert[AuthToken] =
    ConfigConvert.viaString[AuthToken](catchReadError(s => AuthToken(s)), _.value)

  private implicit val pathConverter: ConfigConvert[Path] =
    ConfigConvert.viaString[Path](catchReadError(s => Paths.get(s)), _.toString)

  private implicit val projectRefConverter: ConfigConvert[ProjectRef] =
    ConfigConvert.viaString[ProjectRef](catchReadError(s => ProjectRef(s)), _.toString)

  private implicit val regexConverter: ConfigConvert[Regex] =
    ConfigConvert.viaString[Regex](catchReadError(s => s.r), _.toString)

  val appConfig = AppConfig(
    loadConfigOrThrow[Description](config, "app.description"),
    loadConfigOrThrow[HttpConfig](config, "app.http"),
    loadConfigOrThrow[ClusterConfig](config, "app.cluster"),
    loadConfigOrThrow[PersistenceConfig](config, "app.persistence"),
    loadConfigOrThrow[AttachmentsConfig](config, "app.attachments"),
    loadConfigOrThrow[AdminConfig](config, "app.admin"),
    loadConfigOrThrow[IamConfig](config, "app.iam"),
    loadConfigOrThrow[SparqlConfig](config, "app.sparql"),
    loadConfigOrThrow[ElasticConfig](config, "app.elastic"),
    loadConfigOrThrow[PaginationConfig](config, "app.pagination"),
    loadConfigOrThrow[IndexingConfig](config, "app.indexing"),
    loadConfigOrThrow[KafkaConfig](config, "app.kafka")
  )

}

object Settings extends ExtensionId[Settings] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = Settings

  override def createExtension(system: ExtendedActorSystem): Settings = apply(system.settings.config)

  def apply(config: Config): Settings = new Settings(config)
}
