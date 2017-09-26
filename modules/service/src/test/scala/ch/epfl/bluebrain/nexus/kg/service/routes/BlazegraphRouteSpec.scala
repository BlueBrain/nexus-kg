package ch.epfl.bluebrain.nexus.kg.service.routes

import ch.epfl.bluebrain.nexus.kg.indexing.BlazegraphSpecLike

/**
  * Bundles all suites that depend on a running blazegraph instance.
  */
class BlazegraphRouteSpec extends BlazegraphSpecLike {

  override val nestedSuites = Vector(
    new InstanceRoutesSpec(port))

}
