package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.rdf.Graph
import io.circe.Json

package object resources {
  /**
    * Primary resource representation.
    */
  type Resource  = ResourceF[ProjectRef, Ref, Json]
  /**
    * Materialized resource representation with "flattened" context and "computed" graph
    */
  type ResourceM = ResourceF[ProjectRef, Ref, (Json, Json, Graph)]
}
