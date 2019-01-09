package ch.epfl.bluebrain.nexus.kg.config

import java.nio.file.{Path, Paths}

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import com.typesafe.config.Config
import pureconfig.generic.auto._
import pureconfig.ConvertHelpers.catchReadError
import pureconfig._

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

  private implicit val authTokenConverter: ConfigConvert[AuthToken] =
    ConfigConvert.viaString[AuthToken](catchReadError(s => AuthToken(s)), _.value)

  private implicit val pathConverter: ConfigConvert[Path] =
    ConfigConvert.viaString[Path](catchReadError(s => Paths.get(s)), _.toString)

  private implicit val absoluteIriConverter: ConfigConvert[AbsoluteIri] =
    ConfigConvert.viaString[AbsoluteIri](catchReadError(s => url"$s".value), _.toString)

  val appConfig: AppConfig = loadConfigOrThrow[AppConfig](config, "app")

}

object Settings extends ExtensionId[Settings] with ExtensionIdProvider {

  override def lookup(): ExtensionId[_ <: Extension] = Settings

  override def createExtension(system: ExtendedActorSystem): Settings = apply(system.settings.config)

  def apply(config: Config): Settings = new Settings(config)
}
