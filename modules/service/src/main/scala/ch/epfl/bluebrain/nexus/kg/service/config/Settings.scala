package ch.epfl.bluebrain.nexus.kg.service.config

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit._

import akka.actor._
import akka.http.scaladsl.model.Uri
import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.util.Try

// $COVERAGE-OFF$
class Settings(config: Config) extends Extension {

  // application specific namespace
  private val ns = config.getConfig("app")

  /**
    * Items that refer to the description of the service.
    */
  object Description {
    /**
      * The machine readable name of the service.
      */
    val Name = ns.getString("description.name")
    /**
      * The currently deployed version of the service.
      */
    val Version = BuildInfo.version
    /**
      * The current environment.
      */
    val Environment = Try {
      ns.getString("description.environment")
    } getOrElse {
      val unique = UUID.randomUUID().toString.toLowerCase.replaceAll("-", "")
      s"local-$unique"
    }
    /**
      * The ActorSystem name.
      */
    val ActorSystemName = s"$Name-${Version.replaceAll("\\.", "-")}-$Environment"
  }

  /**
    * Service instance specific settings.
    */
  object Instance {
    /**
      * The default interface to bind to.
      */
    val Interface = ns.getString("instance.interface")
  }

  object Http {
    /**
      * The interface to bind to.
      */
    val Interface = ns.getString("http.interface")
    /**
      * The port to bind to.
      */
    val Port = ns.getInt("http.port")
    /**
      * The default uri prefix.
      */
    val Prefix = ns.getString("http.prefix")
    /**
      * The service public uri.
      */
    val PublicUri = Uri(ns.getString("http.public-uri"))
  }

  /**
    * Items that refer to the runtime configuration of the service.
    */
  object Runtime {
    /**
      * Arbitrary Future completion timeout.
      */
    val Timeout = Duration(ns.getDuration("runtime.default-timeout", MILLISECONDS), MILLISECONDS)
  }

  /**
    * Items that refer to Cluster specific settings.
    */
  object Cluster {
    /**
      * Duration after which actors are shutdown if they have no interaction.
      */
    val PassivationTimeout = Duration(ns.getDuration("cluster.passivation-timeout", MILLISECONDS), MILLISECONDS)
    /**
      * Total number of shards in the cluster.
      */
    val Shards = ns.getInt("cluster.shards")
  }

  object Persistence {
    /**
      * The id of the journal plugin to use.
      */
    val JournalPlugin = ns.getString("persistence.journal.plugin")
    /**
      * The id of the snapshot store plugin to use.
      */
    val SnapshotStorePlugin = ns.getString("persistence.snapshot-store.plugin")
    /**
      * The id of the read journal plugin to use.
      */
    val QueryJournalPlugin = ns.getString("persistence.query-journal.plugin")
  }

  object Attachment {
    /**
      * The root path where the instance attachments are going to be persisted
      */
    val VolumePath = new File(ns.getString("attachment.volume-path")).toPath
    /**
      * The hash algorithm to calculate the attachment's digest
      */
    val HashAlgorithm = ns.getString("attachment.digest-algorithm")
  }

  object Prefixes {
    /**
      * The nexus core vocabulary prefix.
      */
    val CoreVocabulary = Uri(ns.getString("prefixes.core-vocabulary"))
    /**
      * The nexus search vocabulary prefix.
      */
    val SearchVocabulary = Uri(ns.getString("prefixes.search-vocabulary"))
  }

  object Sparql {
    /**
      * The base uri for the sparql endpoint.
      */
    val BaseUri = Uri(ns.getString("sparql.base-uri"))

    /**
      * The default sparql endpoint.
      */
    val Endpoint = Uri(ns.getString("sparql.endpoint"))

    object Domains {
      /**
        * The index name for domain indexing.
        */
      val Index = ns.getString("sparql.domains.index")
      /**
        * The base namespace for domain named graphs.
        */
      val GraphBaseNamespace = Uri(ns.getString("sparql.domains.graph-base-namespace"))
    }

    object Organizations {
      /**
        * The index name for organization indexing.
        */
      val Index = ns.getString("sparql.organizations.index")
      /**
        * The base namespace for organization named graphs.
        */
      val GraphBaseNamespace = Uri(ns.getString("sparql.organizations.graph-base-namespace"))
    }


    object Schemas {
      /**
        * The index name for schema indexing.
        */
      val Index = ns.getString("sparql.schemas.index")
      /**
        * The base namespace for schema named graphs.
        */
      val GraphBaseNamespace = Uri(ns.getString("sparql.schemas.graph-base-namespace"))
    }

    object Instances {
      /**
        * The index name for instance indexing.
        */
      val Index = ns.getString("sparql.instances.index")
      /**
        * The base namespace for instance named graphs.
        */
      val GraphBaseNamespace = Uri(ns.getString("sparql.instances.graph-base-namespace"))
    }
    /**
      * The default page offset.
      */
    val From = ns.getLong("sparql.page-from")

    /**
      * The default page size.
      */
    val Size = ns.getInt("sparql.page-size")

  }

  object Organizations {
    /**
      * Duration after which actors are shutdown if they have no interaction.
      */
    val PassivationTimeout = Duration(ns.getDuration("organizations.passivation-timeout", MILLISECONDS), MILLISECONDS)
  }

  object Domains {
    /**
      * Duration after which actors are shutdown if they have no interaction.
      */
    val PassivationTimeout = Duration(ns.getDuration("domains.passivation-timeout", MILLISECONDS), MILLISECONDS)
  }

  object Schemas {
    /**
      * Duration after which actors are shutdown if they have no interaction.
      */
    val PassivationTimeout = Duration(ns.getDuration("schemas.passivation-timeout", MILLISECONDS), MILLISECONDS)
  }

  object Instances {
    /**
      * Duration after which actors are shutdown if they have no interaction.
      */
    val PassivationTimeout = Duration(ns.getDuration("instances.passivation-timeout", MILLISECONDS), MILLISECONDS)
  }
}

object Settings extends ExtensionId[Settings] with ExtensionIdProvider {
  override def lookup(): ExtensionId[_ <: Extension] = Settings

  override def createExtension(system: ExtendedActorSystem): Settings = new Settings(system.settings.config)
}
// $COVERAGE-ON$