package ch.epfl.bluebrain.nexus.kg.core.contexts

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.circe.Json
import io.circe.parser._
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFFormat._
import org.apache.jena.riot.system.RiotLib
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}
import scala.util.{Failure, Success, Try}
import ch.epfl.bluebrain.nexus.kg.core.contexts.JenaExpander._

/**
  * Utility class to format JSON-LD payloads using Jena.
  *
  * @param contexts the operations bundle to resolve contexts
  * @param F        a MonadError typeclass instance for ''F[_]''
  * @tparam F       the monadic effect type
  */
class JenaExpander[F[_]](contexts: Contexts[F])(implicit F: MonadError[F, Throwable]) {

  /**
    * Expands a single JSON-LD ''value'' given the provided context.
    *
    * @param value the string to expand
    * @param json the json payload which contains a ''@context'' json object
    * @return the expanded string
    */
  def apply(value: String, json: Json): F[String] = {
    contexts.resolve(json).map { expanded =>
      val m = createModel(expanded)
      JenaExpander.expand(value, m)
    }
  }

  /**
    * Formats a JSON-LD payload following the JSON-LD ''expanded'' output variant.
    *
    * @param json the json payload to expand, which contains a ''@context'' json object
    * @return the expanded output
    */
  def apply(json: Json): F[Json] = apply(json, JSONLD_EXPAND_FLAT)

  /**
    * Formats a JSON-LD payload following a provided JSON-LD variant.
    *
    * @param json the json payload to format, which contains a ''@context'' json object
    * @param rdfFormat a Jena [[RDFFormat]] variant
    * @return the formatted output
    */
  def apply(json: Json, rdfFormat: RDFFormat): F[Json] = {
    contexts.resolve(json).flatMap { expanded =>
      val m = createModel(expanded)
      expand(m, rdfFormat)
    }
  }

  private def expand(m: Model, f: RDFFormat): F[Json] = {
    val out = new ByteArrayOutputStream()
    Try {
      val w  = RDFDataMgr.createDatasetWriter(f)
      val g  = DatasetFactory.create(m).asDatasetGraph
      val pm = RiotLib.prefixMap(g)
      w.write(out, g, pm, null, null)
      out.flush()
      parse(out.toString("UTF-8")).toTry
    }.fold(
      err => {
        Try(out.close())
        F.raiseError(err)
      }, {
        case Success(value) => F.pure(value)
        case Failure(err)   => F.raiseError(err)
      }
    )
  }

}

object JenaExpander {

  private[contexts] def expand(value: String, model: Model): String =
    model.expandPrefix(value)

  private[contexts] def createModel(json: Json): Model = {
    val model = ModelFactory.createDefaultModel()
    RDFDataMgr.read(model, new ByteArrayInputStream(json.noSpaces.getBytes), Lang.JSONLD)
    model
  }

  /**
    * Expands a single JSON-LD ''value'' given the provided context.
    *
    * @param value the string to expand
    * @param json the resolved json context
    * @return the expanded string
    */
  def expand(value: String, json: Json): String = {
    val m = createModel(json)
    expand(value, m)
  }
}
