package ch.epfl.bluebrain.nexus.kg.service.operations

import cats.Show
import ch.epfl.bluebrain.nexus.kg.service.types.Named

final case class Id(name: String) extends Named

object Id {
  implicit val showId: Show[Id] = Show.show(_.name)

}
