package ch.epfl.bluebrain.nexus.kg.service.config

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig._
import com.typesafe.config.Config
import pureconfig.ConvertHelpers.catchReadError
import pureconfig.{ConfigConvert, loadConfigOrThrow}

/**
  * Akka settings extension to expose application configuration.  It typically uses the configuration instance of the
  * actor system as the configuration root.
  *
  * @param config the configuration instance to read
  */
@SuppressWarnings(Array("LooksLikeInterpolatedString"))
class Settings(config: Config) extends Extension {

  private implicit val uriConverter: ConfigConvert[Uri] =
    ConfigConvert.viaString[Uri](catchReadError(s => Uri(s)), _.toString)

  private implicit val pathConverter: ConfigConvert[Path] =
    ConfigConvert.viaString[Path](catchReadError(s => Path(s)), _.toString)

  val appConfig = AppConfig(
    loadConfigOrThrow[DescriptionConfig](config, "app.description"),
    loadConfigOrThrow[InstanceConfig](config, "app.instance"),
    loadConfigOrThrow[HttpConfig](config, "app.http"),
    loadConfigOrThrow[RuntimeConfig](config, "app.runtime"),
    loadConfigOrThrow[ClusterConfig](config, "app.cluster"),
    loadConfigOrThrow[PersistenceConfig](config, "app.persistence"),
    loadConfigOrThrow[OperationsConfig](config, "app.operations"),
    loadConfigOrThrow[ProjectsConfig](config, "app.projects"),
    loadConfigOrThrow[SchemasConfig](config, "app.schemas"),
    loadConfigOrThrow[InstancesConfig](config, "app.instances"),
    loadConfigOrThrow[PrefixesConfig](config, "app.prefixes"),
    loadConfigOrThrow[IamConfig](config, "app.iam")
  )

}

object Settings extends ExtensionId[Settings] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = Settings

  override def createExtension(system: ExtendedActorSystem): Settings = new Settings(system.settings.config)

  def apply(config: Config): Settings = new Settings(config)
}
