package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import java.util.regex.Pattern

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.kg.core.Resources
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.{Filter, FilteringSettings}
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class FilteredQuerySpec extends WordSpecLike with Matchers with Resources with EitherValues {

  private val base = "http://localhost/v0"
  private val replacements = Map(Pattern.quote("{{base}}") -> base)
  private implicit val filteringSettings@FilteringSettings(nexusBaseVoc, nexusSearchVoc) =
    FilteringSettings(s"$base/voc/nexus/core", s"$base/voc/nexus/search")

  private val (nxv, nxs) = (Uri(s"$nexusBaseVoc/"), Uri(s"$nexusSearchVoc/"))
  private val prov = Uri("http://www.w3.org/ns/prov#")
  private val rdf = Uri("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
  private val bbpprod = Uri(s"$base/voc/bbp/productionentity/core/")
  private val bbpagent = Uri(s"$base/voc/bbp/agent/core/")

  "A FilteredQuery" should {
    "build the appropriate SPARQL query" when {
      "using a filter" in {
        val json = jsonContentOf("/query/builder/filter-only.json", replacements)
        val filter = json.as[Filter].right.value
        val pagination = Pagination(13, 17)
        val expectedWhere =
          s"""
             |?s <${prov}wasDerivedFrom> ?var_1 .
             |?s <${nxv}rev> ?var_2 .
             |FILTER ( ?var_1 = <http://localhost/v0/bbp/experiment/subject/v0.1.0/073b4529-83a8-4776-a5a7-676624bfad90> && ?var_2 <= 5 )
             |
             |OPTIONAL { ?s <${nxv}version> ?var_3 . }
             |OPTIONAL { ?s <${nxv}version> ?var_4 . }
             |FILTER ( ?var_3 = "v1.0.0" || ?var_4 = "v1.0.1" )
             |
             |?s <${nxv}deprecated> ?var_5 .
             |?s <${rdf}type> ?var_6 .
             |FILTER ( ?var_5 != false && ?var_6 IN (<${prov}Entity>, <${bbpprod}Circuit>) )
             |
             |?s <${nxv}version> ?var_7 .
             |?s <${nxv}rev> ?var_8 .
             |FILTER NOT EXISTS {
             |?s <${nxv}version> ?var_7 .
             |?s <${nxv}rev> ?var_8 .
             |FILTER ( ?var_7 = "v1.0.2" || ?var_8 <= 2 )
             |}
             |
             |OPTIONAL { ?s <${prov}wasAttributedTo> ?var_9 . }
             |OPTIONAL { ?s <${prov}wasAttributedTo> ?var_10 . }
             |FILTER ( ?var_9 = <${bbpagent}sy> || ?var_10 = <${bbpagent}dmontero> )
             |FILTER NOT EXISTS {
             |?s <${prov}wasAttributedTo> ?var_9 .
             |?s <${prov}wasAttributedTo> ?var_10 .
             |FILTER ( ?var_9 = <${bbpagent}sy> || ?var_10 = <${bbpagent}dmontero> )
             |}
             |""".stripMargin
        val expected =
          s"""
             |SELECT ?total ?s
             |WITH {
             |  SELECT ?s
             |  WHERE {
             |$expectedWhere
             |  }
             |} AS %resultSet
             |WHERE {
             |  {
             |    SELECT (COUNT(DISTINCT ?s) AS ?total)
             |    WHERE { INCLUDE %resultSet }
             |  }
             |  UNION
             |  {
             |    SELECT *
             |    WHERE { INCLUDE %resultSet }
             |    LIMIT 17
             |    OFFSET 13
             |  }
             |}""".stripMargin
        val result = FilteredQuery(filter, pagination)
        result shouldEqual expected
      }
    }
  }

}
