package ch.epfl.bluebrain.nexus.kg.indexing

import ch.epfl.bluebrain.nexus.commons.es.server.embed.ElasticServer
import ch.epfl.bluebrain.nexus.kg.indexing.organizations.OrganizationEsIndexerSpec

class ElasticSpecLike extends ElasticServer {
  override val nestedSuites = Vector(
    new OrganizationEsIndexerSpec(esUri)
  )
}
