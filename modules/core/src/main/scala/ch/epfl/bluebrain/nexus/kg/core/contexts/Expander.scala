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

class JenaExpander[F[_]](implicit contexts: Contexts[F], F: MonadError[F, Throwable])
    extends StringExpander[F]
    with JsonExpander[F] {

  override def apply(value: String, json: Json): F[String] = {
    contexts.expand(json).map { expanded =>
      val m = createModel(expanded)
      expand(value, m)
    }
  }

  override def apply(json: Json): F[Json] = {
    contexts.expand(json).flatMap { expanded =>
      val m = createModel(expanded)
      expand(m, RDFFormat.JSONLD_EXPAND_FLAT)
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
      val w = RDFDataMgr.createDatasetWriter(f)
      val g = DatasetFactory.create(m).asDatasetGraph
      val pm = RiotLib.prefixMap(g)
      w.write(out, g, pm, null, null)
      out.flush()
      parse(out.toString("UTF-8")).toTry
    }.fold(
      err => {
        Try(out.close())
        F.raiseError(err)
      },
      {
        case (Success(value)) => F.pure(value)
        case (Failure(err)) => F.raiseError(err)
      }
    )
  }

}

object JenaExpander {
  final def apply[F[_]](v: String, j: Json)(implicit contexts: Contexts[F], F: MonadError[F, Throwable]): F[String] =
    new JenaExpander().apply(v, j)

  final def apply[F[_]](j: Json)(implicit contexts: Contexts[F], F: MonadError[F, Throwable]): F[Json] =
    new JenaExpander().apply(j)
}
