package ch.epfl.bluebrain.nexus.kg.service.config

import java.nio.file.Paths

import com.typesafe.config.{Config, ConfigFactory}

object ExternalConfig {

  /**
    * Builds a configuration object by stacking an external file configuration specified via the env variable or system
    * property on top of the default application configuration. It also resolves the stacked configuration.
    *
    * @param env  the name of the environment variable to lookup
    * @param prop the name of the system property to lookup
    */
  final def apply(env: String, prop: String): Config = {
    val cfg = sys.env.get(env) orElse sys.props.get(prop) map { str =>
      val file = Paths.get(str).toAbsolutePath.toFile
      ConfigFactory.parseFile(file)
    } getOrElse ConfigFactory.empty()
    (cfg withFallback ConfigFactory.load()).resolve()
  }

}
