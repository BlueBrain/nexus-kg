package ch.epfl.bluebrain.nexus.kg

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.http.ContextUri
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris

package object service {

  val prefixes: PrefixUris = new PrefixUris {
    override val CoreContext = ContextUri("http://localhost/v0/contexts/nexus/core/resource/v0.1.0")

    override val StandardsContext = ContextUri("http://localhost/v0/contexts/nexus/core/standards/v0.1.0")

    override val LinksContext = ContextUri("http://localhost/v0/contexts/nexus/core/links/v0.1.0")

    override val SearchContext = ContextUri("http://localhost/v0/contexts/nexus/core/search/v0.1.0")

    override val DistributionContext = ContextUri("http://localhost/v0/contexts/nexus/core/distribution/v0.1.0")

    override val ErrorContext = ContextUri("http://localhost/v0/contexts/nexus/core/error/v0.1.0")

    override val CoreVocabulary = Uri("http://localhost/v0/vocabs/nexus/core/terms/v0.1.0")

    override val SearchVocabulary = Uri("http://localhost/v0/vocabs/nexus/search/terms/v0.1.0")
  }
}
