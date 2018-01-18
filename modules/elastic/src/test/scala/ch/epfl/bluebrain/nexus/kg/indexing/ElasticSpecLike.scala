package ch.epfl.bluebrain.nexus.kg.indexing

import ch.epfl.bluebrain.nexus.commons.es.server.embed.ElasticServer
import ch.epfl.bluebrain.nexus.kg.indexing.domains.DomainElasticIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.organizations.OrganizationElasticIndexerSpec

class ElasticSpecLike extends ElasticServer {
  override val nestedSuites = Vector(
    new OrganizationElasticIndexerSpec(esUri),
    new DomainElasticIndexerSpec(esUri)
  )
}
