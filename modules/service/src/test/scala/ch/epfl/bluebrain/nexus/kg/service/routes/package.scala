package ch.epfl.bluebrain.nexus.kg.service

import ch.epfl.bluebrain.nexus.kg.service.BootstrapService.kgOrderedKeys

package object routes {
  implicit val orderedKeys = kgOrderedKeys
}
