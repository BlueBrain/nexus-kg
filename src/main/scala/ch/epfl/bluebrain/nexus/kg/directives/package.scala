package ch.epfl.bluebrain.nexus.kg

import akka.http.scaladsl.unmarshalling.Unmarshaller
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives.toIri
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

package object directives {
  private[directives] implicit def absoluteIriFromStringUnmarshaller(
      implicit project: Project): Unmarshaller[String, AbsoluteIri] =
    Unmarshaller.strict[String, AbsoluteIri] { string =>
      toIriOrElseBase(string) match {
        case Some(iri) => iri
        case x         => throw new IllegalArgumentException(s"'$x' is not a valid AbsoluteIri value")
      }
    }

  private[directives] implicit def vocabAbsoluteIriFromStringUnmarshaller(
      implicit project: Project): Unmarshaller[String, VocabAbsoluteIri] =
    Unmarshaller.strict[String, VocabAbsoluteIri] { string =>
      toIriOrElseVocab(string) match {
        case Some(iri) => VocabAbsoluteIri(iri)
        case x         => throw new IllegalArgumentException(s"'$x' is not a valid AbsoluteIri value")
      }
    }

  private def toIriOrElseBase(s: String)(implicit project: Project): Option[AbsoluteIri] =
    toIri(s) orElse Iri.absolute(project.base.asString + s).toOption

  private def toIriOrElseVocab(s: String)(implicit project: Project): Option[AbsoluteIri] =
    toIri(s) orElse Iri.absolute(project.vocab.asString + s).toOption
}
