package ch.epfl.bluebrain.nexus.kg.service.io

import io.circe.Printer

object PrinterSettings {
  implicit val printer = Printer.noSpaces.copy(dropNullValues = true)
}
