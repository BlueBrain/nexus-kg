package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.ContentTypes.`application/octet-stream`
import akka.http.scaladsl.model.HttpCharsets._
import akka.http.scaladsl.model.{ContentType, MediaType}
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes._

/**
  * Enumeration of output format types.
  */
sealed trait OutputFormat extends Product with Serializable {

  /**
    * @return the format name
    */
  def name: String

}

/**
  * Enumeration of non-binary output format types.
  */
sealed trait NonBinaryOutputFormat extends OutputFormat {
  def contentType: ContentType.NonBinary
}

/**
  * Enumeration of text output format types.
  */
sealed trait TextOutputFormat extends NonBinaryOutputFormat

/**
  * Enumeration of JSON-LD output format types.
  */
sealed trait JsonLDOutputFormat extends NonBinaryOutputFormat {
  val contentType: ContentType.NonBinary = `application/ld+json`
}

object OutputFormat {

  /**
    * JSON-LD compacted output
    */
  final case object Compacted extends JsonLDOutputFormat {
    val name = "compacted"
  }

  /**
    * JSON-LD expanded output
    */
  final case object Expanded extends JsonLDOutputFormat {
    val name = "expanded"
  }

  /**
    * triples output
    */
  final case object Triples extends TextOutputFormat {
    val name        = "triples"
    val contentType = `application/ntriples`
  }

  /**
    * DOT language output
    */
  final case object DOT extends TextOutputFormat {
    val name        = "dot"
    val contentType = MediaType.textWithFixedCharset("vnd.graphviz", `UTF-8`, "dot")
  }

  /**
    * Binary output
    */
  final case object Binary extends OutputFormat {
    val name                            = "binary"
    val contentType: ContentType.Binary = `application/octet-stream`
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
    else if (name == Triples.name) Some(Triples)
    else if (name == DOT.name) Some(DOT)
    else if (name == Binary.name) Some(Binary)
    else None
}
