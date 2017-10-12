package ch.epfl.bluebrain.nexus.kg.indexing.filtering

import java.io.ByteArrayInputStream

import akka.http.scaladsl.model.Uri
import cats.Eval
import cats.syntax.either._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Expr.{ComparisonExpr, InExpr, LogicalExpr, NoopExpr}
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Op._
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.PathProp.UriPath
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.Term.{LiteralTerm, TermCollection, UriTerm}
import io.circe._
import org.apache.jena.graph.{Graph, Node}
import org.apache.jena.rdf.model._
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.jena.sparql.path.{Path, PathParser}
import cats.instances.try_._
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

    def asUriPath(graph: Graph, node: Node): Try[PathProp] = asUri(graph, node).map(UriPath)

    def asPath(graph: Graph, node: Node): Try[PathProp] = {
      val builder = PathPropBuilder[Try, Path]
      Try(PathParser.parse(node.getLiteralLexicalForm, graph.getPrefixMapping))
        .flatMap(path => builder(path))
    }


    def extractPath(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[Option[PathProp]]] = {
      val history = cursor.downField("path").history
      Eval.now {
        objectsOfProperty(graph, node, pathProp) match {
          case Nil         =>
            Right(None)
          case head :: Nil =>
            asUriPath(graph, head).orElse(asPath(graph, head))
              .map(p => Right(Some(p)))
              .getOrElse(Left(DecodingFailure("Unable to parse 'path' as an uri", history)))
          case _ :: _      =>
            Left(DecodingFailure("A filter expression can contain at most a single path", history))
        }
      }
    }

    def extractInOrComparisonOp(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[Either[In, ComparisonOp]]] = {
      val history = cursor.downField("op").history
      Eval.now {
        objectsOfProperty(graph, node, opProp) match {
          case head :: Nil =>
            lazy val failure = Left(DecodingFailure("A filter expression with a 'path' value must present a comparison or in operator", history))
            val opt = Try(head.getLiteral.getLexicalForm).toOption
            opt.map { str =>
              (In.fromString(str), ComparisonOp.fromString(str)) match {
                case (Some(In), None) => Right(Left(In))
                case (None, Some(co)) => Right(Right(co))
                case _                => failure
              }
            }.getOrElse(failure)
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
              .getOrElse(Left(DecodingFailure("A filter expression without a 'path' value must present a logical operator", history)))
          case _           =>
            Left(DecodingFailure("A filter expression must always define an 'op' value", history))
        }
      }
    }

    def extractTermCollection(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[TermCollection]] = {
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
                      else Try(LiteralTerm({
                        val lit = el.getLiteral
                        if (classOf[java.lang.Number].isInstance(lit.getValue) || classOf[java.lang.Boolean].isInstance(lit.getValue)) lit.getLexicalForm
                        else s""""${lit.getLexicalForm}""""
                      }))
                    }
                    .toEither
                    .leftMap(_ => failure)
                    .map(term => term :: list)
            }
        termList match {
          case Left(df)           => Left(df)
          case Right(Nil)         => Left(failure)
          case Right(collection)  => Right(TermCollection(collection))
        }
      }
    }

    def extractTermValue(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[Term]] = {
      val failureMessage = "A filter expression with a path must define a single 'value' as uri or literal"
      lazy val failure = DecodingFailure(failureMessage, cursor.downField("value").history)
      extractTermCollection(graph, node, cursor).map(_.flatMap { terms =>
        terms.values match {
          case head :: Nil => Right(head)
          case _           => Left(failure)
        }
      })
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
              else {
                val exprCursor = cursor.downField("value").downN(idx)
                decodeExpr(graph, el, exprCursor).map {
                  case Left(df)                    => Left(df)
                  case Right(expr: InExpr)         => Right(expr :: list)
                  case Right(expr: ComparisonExpr) => Right(expr :: list)
                  case Right(_: LogicalExpr)       => Left(DecodingFailure("Logical expression cannot be nested further", exprCursor.history))
                  case Right(NoopExpr)             => Left(DecodingFailure("Impossible case", exprCursor.history))
                }.value
              }
          }.map(_.reverse)
      }
    }

    def extractNestedExprs(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[List[Expr]]] = {
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
          }.map(_.reverse)
      }
    }

    def decodeExpr(graph: Graph, node: Node, cursor: ACursor): Eval[Decoder.Result[Expr]] = {
      extractPath(graph, node, cursor).flatMap {
        case Left(df)          => Eval.now(Left(df))
        case Right(Some(path)) =>
          extractInOrComparisonOp(graph, node, cursor).flatMap {
            case Left(df)         => Eval.now(Left(df))
            case Right(Left(In))  =>
              extractTermCollection(graph, node, cursor).map(_.map(terms => InExpr(path, terms)))
            case Right(Right(op)) =>
              extractTermValue(graph, node, cursor).map(_.map(term => ComparisonExpr(op, path, term)))
          }
        case Right(None)       =>
          extractLogicalOp(graph, node, cursor).flatMap {
            case Left(df)  => Eval.now(Left(df))
            case Right(op) => op match {
              case Or | Xor | Not =>
                extractExprs(graph, node, cursor).map {
                  case Left(df)     => Left(df)
                  case Right(Nil)   => Left(DecodingFailure("A filter expression with a logical operator must define at least a value", cursor.downField("value").history))
                  case Right(exprs) => Right(LogicalExpr(op, exprs))
                }
              case And            =>
                extractNestedExprs(graph, node, cursor).map {
                  case Left(df)     => Left(df)
                  case Right(Nil)   => Left(DecodingFailure("A filter expression with a logical operator must define at least a value", cursor.downField("value").history))
                  case Right(exprs) => Right(LogicalExpr(And, exprs))
                }
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