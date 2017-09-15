package ch.epfl.bluebrain.nexus.kg.indexing.filtering

import akka.http.scaladsl.model.Uri

/**
  * Enumeration type for all filtering term values.
  */
sealed trait Term extends Product with Serializable

object Term {
  /**
    * A term represented as an uri.
    *
    * @param value the underlying uri.
    */
  final case class UriTerm(value: Uri) extends Term

  /**
    * A term represented as in its string form.
    *
    * @param lexicalForm the underlying lexical form
    */
  final case class LiteralTerm(lexicalForm: String) extends Term

  /**
    * A term that describes a collection of terms
    * @param values the underlying terms
    */
  final case class TermCollection(values: Set[Term]) extends Term
}