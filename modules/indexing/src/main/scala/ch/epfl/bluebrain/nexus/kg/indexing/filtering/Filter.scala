package ch.epfl.bluebrain.nexus.kg.indexing.filtering

import java.io.ByteArrayInputStream

import akka.http.scaladsl.model.Uri
import cats.Eval
import cats.syntax.either._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.{ComparisonExpr, LogicalExpr}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.{LiteralTerm, TermCollection, UriTerm}
import io.circe._
import org.apache.jena.graph.{Graph, Node}
import org.apache.jena.rdf.model._
import org.apache.jena.riot.{Lang, RDFDataMgr}

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

/**
  * Filter representation that wraps a single filtering expression.
  *
  * @param expr the expression to be evaluated
  */
final case class Filter(expr: Expr)

object Filter {

  /**
    * Builds the default context for filters based on the provided settings.
    *
    * @param settings the settings to be used
    */
  final def defaultContext(settings: FilteringSettings): Json = Json.obj(
    "nxv"    -> Json.fromString(s"${settings.nexusBaseVoc}/"),
    "nxs"    -> Json.fromString(s"${settings.nexusSearchVoc}/"),
    "filter" -> Json.fromString("nxs:filter"),
    "path"   -> Json.fromString("nxs:path"),
    "value"  -> Json.fromString("nxs:value"),
    "op"     -> Json.fromString("nxs:operator"))

  /**
    * A filter decoder implementation that traverses the json tree and builds the filter expressions.
    *
    * @param settings the filtering settings
    */
  final implicit def filterDecoder(implicit settings: FilteringSettings): Decoder[Filter] = {
    val defaultCtx = defaultContext(settings)
    val voc = s"${settings.nexusSearchVoc}/"
    val filterProp = ResourceFactory.createProperty(voc, "filter")
    val pathProp = ResourceFactory.createProperty(voc, "path")
    val valueProp = ResourceFactory.createProperty(voc, "value")
    val opProp = ResourceFactory.createProperty(voc, "operator")

    def objectsOfProperty(graph: Graph, node: Node, prop: Property): List[Node] =
      graph.find(node, prop.asNode(), Node.ANY).asScala.map(_.getObject).toList

    def asUri(graph: Graph, node: Node): Try[Uri] =
      Try(Uri(graph.getPrefixMapping.expandPrefix(node.getLiteralLexicalForm)))
        .filter(uri => uri.isAbsolute && uri.toString().indexOf("/") > -1)

    def extractPath(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[Option[UriTerm]]] = {
      val history = cursor.downField("path").history
      Eval.now {
        objectsOfProperty(graph, node, pathProp) match {
          case Nil         =>
            Right(None)
          case head :: Nil =>
            asUri(graph, head)
              .map(uri => Right(Some(UriTerm(uri))))
              .getOrElse(Left(DecodingFailure("Unable to parse 'path' as an uri", history)))
          case _ :: _      =>
            Left(DecodingFailure("A filter expression can contain at most a single path", history))
        }
      }
    }

    def extractComparisonOp(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[ComparisonOp]] = {
      val history = cursor.downField("op").history
      Eval.now {
        objectsOfProperty(graph, node, opProp) match {
          case head :: Nil =>
            Try(head.getLiteral.getLexicalForm).toOption
              .flatMap(str => ComparisonOp.fromString(str))
              .map(op => Right(op))
              .getOrElse(Left(DecodingFailure(s"A filter expression with a 'path' value must present a comparison operator", history)))
          case _           =>
            Left(DecodingFailure("A filter expression must always define an 'op' value", history))
        }
      }
    }

    def extractLogicalOp(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[LogicalOp]] = {
      val history = cursor.downField("op").history
      Eval.now {
        objectsOfProperty(graph, node, opProp) match {
          case head :: Nil =>
            Try(head.getLiteral.getLexicalForm).toOption
              .flatMap(str => LogicalOp.fromString(str))
              .map(op => Right(op))
              .getOrElse(Left(DecodingFailure(s"A filter expression without a 'path' value must present a logical operator", history)))
          case _           =>
            Left(DecodingFailure("A filter expression must always define an 'op' value", history))
        }
      }
    }

    def extractTermValue(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[Term]] = {
      val failureMessage = "A filter expression with a path must define 'value' as uri, literal or an array of values"
      lazy val failure = DecodingFailure(failureMessage, cursor.downField("value").history)
      Eval.now {
        val termList =
          objectsOfProperty(graph, node, valueProp)
            .foldLeft[Decoder.Result[List[Term]]](Right(Nil)) {
              case (Left(df), _)     => Left(df)
              case (Right(list), el) =>
                if (el.isBlank) Left(failure)
                else
                  asUri(graph, el)
                    .map(uri => UriTerm(uri))
                    .orElse {
                      if (el.getLiteralLexicalForm.indexOf(":") > -1) Failure(failure)
                      else Try(LiteralTerm(el.getLiteral.getLexicalForm))
                    }
                    .toEither
                    .leftMap(_ => failure)
                    .map(term => term :: list)
            }
        termList match {
          case Left(df)           => Left(df)
          case Right(Nil)         => Left(failure)
          case Right(head :: Nil) => Right(head)
          case Right(collection)  => Right(TermCollection(collection.toSet))
        }
      }
    }

    def extractExprs(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[List[Expr]]] = {
      val history = cursor.downField("value").history
      Eval.now {
        objectsOfProperty(graph, node, valueProp).reverse.zipWithIndex
          .foldLeft[Decoder.Result[List[Expr]]](Right(Nil)) {
            case (Left(df), _)            => Left(df)
            case (Right(list), (el, idx)) =>
              if (!el.isBlank)
                Left(DecodingFailure("Values for logical operators need to be filtering expressions", history))
              else
                decodeExpr(graph, el, cursor.downField("value").downN(idx)).map {
                  case Left(df)    => Left(df)
                  case Right(expr) => Right(expr :: list)
                }.value
          }
      }
    }

    def decodeExpr(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[Expr]] = {
      extractPath(graph, node, cursor).flatMap {
        case Left(df)          => Eval.now(Left(df))
        case Right(Some(path)) =>
          extractComparisonOp(graph, node, cursor).flatMap {
            case Left(df)  => Eval.now(Left(df))
            case Right(op) =>
              extractTermValue(graph, node, cursor).map {
                case Left(df)    => Left(df)
                case Right(term) => Right(ComparisonExpr(op, path, term))
              }
          }
        case Right(None)       =>
          extractLogicalOp(graph, node, cursor).flatMap {
            case Left(df)  => Eval.now(Left(df))
            case Right(op) =>
              extractExprs(graph, node, cursor).map {
                case Left(df)     => Left(df)
                case Right(Nil)   => Left(DecodingFailure("A filter expression with a logical operator must define at least a value", cursor.downField("value").history))
                case Right(exprs) => Right(LogicalExpr(op, exprs.toSet))
              }
          }
      }
    }

    Decoder.instance { hcursor =>
      val json = hcursor.value
      val str = json.deepMerge(Json.obj("@context" -> defaultCtx)).noSpaces
      val model = ModelFactory.createDefaultModel()
      RDFDataMgr.read(model, new ByteArrayInputStream(str.getBytes), Lang.JSONLD)
      val graph = model.getGraph

      val filters = objectsOfProperty(graph, Node.ANY, filterProp)
      val filterCursor = hcursor.downField("filter")
      filters match {
        case head :: Nil => decodeExpr(model.getGraph, head, filterCursor).value.map(expr => Filter(expr))
        case _ :: _      =>
          Left(DecodingFailure("A single filter value accepted", filterCursor.history))
        case Nil         =>
          Left(DecodingFailure("A filter value is required", filterCursor.history))
      }
    }
  }
}