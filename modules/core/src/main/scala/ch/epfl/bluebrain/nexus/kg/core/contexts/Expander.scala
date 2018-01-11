package ch.epfl.bluebrain.nexus.kg.core.contexts

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import io.circe.Json
import io.circe.parser._
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.system.RiotLib
import org.apache.jena.riot.{Lang, RDFDataMgr, RDFFormat}

import scala.util.{Failure, Success, Try}

sealed trait StringExpander[F[_]] extends ((String, Json) => F[String])

sealed trait JsonExpander[F[_]] extends (Json => F[Json])

class JenaExpander[F[_]](contexts: Contexts[F])(implicit F: MonadError[F, Throwable])
    extends StringExpander[F]
    with JsonExpander[F] {

  override def apply(value: String, json: Json): F[String] = {
    contexts.resolve(json).map { expanded =>
      val m = createModel(expanded)
      expand(value, m)
    }
  }

  def apply(json: Json): F[Json] = apply(json, RDFFormat.JSONLD_EXPAND_FLAT)

  def apply(json: Json, rdfFormat: RDFFormat): F[Json] = {
    contexts.resolve(json).flatMap { expanded =>
      val m = createModel(expanded)
      expand(m, rdfFormat)
    }
  }

  private def createModel(json: Json): Model = {
    val model = ModelFactory.createDefaultModel()
    RDFDataMgr.read(model, new ByteArrayInputStream(json.noSpaces.getBytes), Lang.JSONLD)
    model
  }

  private def expand(value: String, model: Model): String =
    model.expandPrefix(value)

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
        case (Success(value)) => F.pure(value)
        case (Failure(err))   => F.raiseError(err)
      }
    )
  }

}
