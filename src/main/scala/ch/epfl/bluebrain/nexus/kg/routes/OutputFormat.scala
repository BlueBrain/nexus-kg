package ch.epfl.bluebrain.nexus.kg.routes

/**
  * Enumeration of output format types.
  */
sealed trait OutputFormat extends Product with Serializable {

  /**
    * @return the format name
    */
  def name: String
}

object OutputFormat {

  /**
    * JSON-LD compacted output
    */
  final case object Compacted extends OutputFormat {
    val name = "compacted"
  }

  /**
    * JSON-LD expanded output
    */
  final case object Expanded extends OutputFormat {
    val name = "expanded"
  }

  /**
    * Attempts to build an output format from a name.
    *
    * @param name the output format name
    * @return Some(output) if the name matches some of the existing output formats,
    *         None otherwise
    */
  final def apply(name: String): Option[OutputFormat] =
    if (name == Compacted.name) Some(Compacted)
    else if (name == Expanded.name) Some(Expanded)
    else None
}
