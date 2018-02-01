package ch.epfl.bluebrain.nexus.kg.core.queries.filtering

import java.util.regex.Pattern

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr.{ComparisonExpr, InExpr, LogicalExpr}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op._
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.{AlternativeSeqPath, PathZeroOrOne, SeqPath, UriPath}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.{LiteralTerm, TermCollection, UriTerm}
import io.circe.Json
import org.scalatest.{EitherValues, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Filter._
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.FilterSpec._

class FilterSpec extends WordSpecLike with Matchers with Resources with EitherValues {

  private val base         = "http://localhost/v0"
  private val replacements = Map(Pattern.quote("{{base}}") -> base)

  val nexusBaseVoc: Uri = s"https://bbp-nexus.epfl.ch/vocabs/nexus/core/terms/v0.1.0/"
  private val context = jsonContentOf("/schemas/nexus/core/search/search_expanded.json",
                                      Map(Pattern.quote("{{vocab}}") -> nexusBaseVoc.toString))
  private implicit val filteringSettings = FilteringSettings(nexusBaseVoc, context)

  private val prov     = Uri("http://www.w3.org/ns/prov#")
  private val rdf      = Uri("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
  private val bbpprod  = Uri(s"$base/voc/bbp/productionentity/core/")
  private val bbpagent = Uri(s"$base/voc/bbp/agent/core/")

  "A Filter" should {

    "be parsed correctly from json" when {

      "using a single comparison" in {
        val json =
          jsonContentOf("/filtering/single-comparison.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expected      = Filter(ComparisonExpr(Eq, UriPath(s"${nexusBaseVoc}deprecated"), LiteralTerm("false")))
        filter.as[Filter] shouldEqual Right(expected)
      }

      "using a single comparison property path" in {
        val json =
          jsonContentOf("/filtering/single-comparison-date.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expected = Filter(
          ComparisonExpr(Eq,
                         UriPath(s"http://www.w3.org/ns/prov#startedAtTime"),
                         LiteralTerm(""""2017-10-07T16:00:00-05:00"""")))
        filter.as[Filter] shouldEqual Right(expected)
      }

      "using nested comparisons" in {
        val json =
          jsonContentOf("/filtering/nested-comparison.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expected = Filter(
          LogicalExpr(
            And,
            List(
              ComparisonExpr(Eq,
                             UriPath(s"${prov}wasDerivedFrom"),
                             UriTerm(s"$base/bbp/experiment/subject/v0.1.0/073b4529-83a8-4776-a5a7-676624bfad90")),
              ComparisonExpr(Ne, UriPath(s"${nexusBaseVoc}deprecated"), LiteralTerm("false")),
              InExpr(UriPath(s"${rdf}type"),
                     TermCollection(List(UriTerm(s"${prov}Entity"), UriTerm(s"${bbpprod}Circuit")))),
              ComparisonExpr(Lte, UriPath(s"${nexusBaseVoc}rev"), LiteralTerm("5")),
              LogicalExpr(
                Xor,
                List(
                  ComparisonExpr(Eq, UriPath(s"${prov}wasAttributedTo"), UriTerm(s"${bbpagent}sy")),
                  ComparisonExpr(Eq, UriPath(s"${prov}wasAttributedTo"), UriTerm(s"${bbpagent}dmontero"))
                )
              )
            )
          ))
        filter.as[Filter] shouldEqual Right(expected)
      }

      "using nested comparisons with property path" in {
        val json          = jsonContentOf("/filtering/nested-comparison-property-path.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val nx            = s"$base/voc/nexus/core/"
        val expected = Filter(
          LogicalExpr(
            And,
            List(
              ComparisonExpr(Eq,
                             AlternativeSeqPath(SeqPath(UriPath(s"${nx}schema"), PathZeroOrOne(s"${nx}schemaGroup")),
                                                UriPath(s"${nx}name")),
                             LiteralTerm(""""subject"""")),
              ComparisonExpr(Eq,
                             UriPath(s"${prov}wasDerivedFrom"),
                             UriTerm(s"$base/bbp/experiment/subject/v0.1.0/073b4529-83a8-4776-a5a7-676624bfad90")),
              ComparisonExpr(Ne, UriPath(s"${nexusBaseVoc}deprecated"), LiteralTerm("false")),
              InExpr(UriPath(s"${rdf}type"),
                     TermCollection(List(UriTerm(s"${prov}Entity"), UriTerm(s"${bbpprod}Circuit")))),
              ComparisonExpr(Lte, UriPath(s"${nexusBaseVoc}rev"), LiteralTerm("5")),
              LogicalExpr(
                Xor,
                List(
                  ComparisonExpr(Eq, UriPath(s"${prov}wasAttributedTo"), UriTerm(s"${bbpagent}sy")),
                  ComparisonExpr(Eq, UriPath(s"${prov}wasAttributedTo"), UriTerm(s"${bbpagent}dmontero"))
                )
              )
            )
          ))
        filter.as[Filter] shouldEqual Right(expected)
      }

      "defining a context that's conflicting with the expected one" in {
        val json =
          jsonContentOf("/filtering/conflicting-context.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expected      = Filter(ComparisonExpr(Eq, UriPath(s"${nexusBaseVoc}deprecated"), LiteralTerm("false")))
        filter.as[Filter] shouldEqual Right(expected)
      }

      "filtering on revisions which are greater than 0 and lower than 10 and other field greater or equal than 10" in {
        val json          = jsonContentOf("/filtering/rev-boundaries.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expected = Filter(
          LogicalExpr(
            And,
            List(
              ComparisonExpr(Gt, UriPath(s"${nexusBaseVoc}rev"), LiteralTerm("0")),
              ComparisonExpr(Lt, UriPath(s"${nexusBaseVoc}rev"), LiteralTerm("10")),
              ComparisonExpr(Gte, UriPath(s"${nexusBaseVoc}other"), LiteralTerm("10"))
            )
          ))
        filter.as[Filter] shouldEqual Right(expected)
      }
    }

    "fail to parse from a json" when {

      "using a nested or" in {
        val json            = jsonContentOf("/filtering/nested-or.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(value)/DownN(4)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a wrong path property" in {
        val json            = jsonContentOf("/filtering/invalid-property-path.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(value)/DownN(0)/DownField(path)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a nested xor" in {
        val json            = jsonContentOf("/filtering/nested-xor.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(value)/DownN(4)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a nested not" in {
        val json            = jsonContentOf("/filtering/nested-not.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(value)/DownN(4)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a multi value for comparison ops" in {
        val json =
          jsonContentOf("/filtering/single-term-value.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(value)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using an incorrect comparison op" in {
        val json =
          jsonContentOf("/filtering/incorrect-comparison-op.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expectedHistory =
          "DownField(value)/DownN(4)/DownField(value)/DownN(0)/DownField(op)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a missing comparison op expression" in {
        val json =
          jsonContentOf("/filtering/missing-comparison-op.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expectedHistory =
          "DownField(value)/DownN(4)/DownField(value)/DownN(0)/DownField(op)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using an incorrect logical op" in {
        val json =
          jsonContentOf("/filtering/incorrect-logical-op.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expectedHistory =
          "DownField(value)/DownN(4)/DownField(op)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a missing logical op expression" in {
        val json =
          jsonContentOf("/filtering/missing-logical-op.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expectedHistory =
          "DownField(value)/DownN(4)/DownField(op)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a logical op and an empty value" in {
        val json =
          jsonContentOf("/filtering/logical-op-empty-value.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(value)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a logical non-nested op and an empty value" in {
        val json =
          jsonContentOf("/filtering/logical-non-nested-op-empty-value.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(value)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a logical op with a non blank node value" in {
        val json =
          jsonContentOf("/filtering/logical-op-string-value.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(value)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a non uri path" in {
        val json            = jsonContentOf("/filtering/non-uri-path.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(path)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a multiple paths" in {
        val json            = jsonContentOf("/filtering/multiple-paths.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(path)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a blank node as a term value" in {
        val json =
          jsonContentOf("/filtering/term-value-blank-node.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expectedHistory =
          "DownField(value)/DownN(2)/DownField(value)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a blank node as a term value on non-nested" in {
        val json =
          jsonContentOf("/filtering/term-value-non-blank-non-nested.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expectedHistory =
          "DownField(value)/DownN(4)/DownField(value)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a missing term value" in {
        val json =
          jsonContentOf("/filtering/term-value-missing.json", replacements)
        val (filter, ctx) = contextAndFilter(json)
        implicit val _    = filterDecoder(ctx)
        val expectedHistory =
          "DownField(value)/DownN(2)/DownField(value)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using a missing filter" in {
        val json            = jsonContentOf("/filtering/missing-filter.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = "DownField(op)"
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }

      "using multiple filters" in {
        val json            = jsonContentOf("/filtering/multiple-filters.json", replacements)
        val (filter, ctx)   = contextAndFilter(json)
        implicit val _      = filterDecoder(ctx)
        val expectedHistory = ""
        filter
          .as[Filter]
          .left
          .value
          .history
          .reverse
          .mkString("/") shouldEqual expectedHistory
      }
    }
  }

}

object FilterSpec {
  def contextAndFilter(filterWithContext: Json)(implicit filteringSettings: FilteringSettings): (Json, Json) = {
    val filter = filterWithContext.hcursor.get[Json]("filter").toOption.getOrElse(Json.obj())
    val ctx = filterWithContext.hcursor
      .get[Json]("@context")
      .map(_.deepMerge(filteringSettings.ctx))
      .getOrElse(filteringSettings.ctx)
    filter -> ctx
  }

}
