package ch.epfl.bluebrain.nexus.kg.indexing.query

import ch.epfl.bluebrain.nexus.commons.types.search.{Sort, SortList}
import ch.epfl.bluebrain.nexus.kg.indexing.query.SortListSparql._
import org.scalatest.{Matchers, WordSpecLike}

class SortListSparqlSpec extends WordSpecLike with Matchers {

  "A SortListSparql" should {
    val base = "http://localhost/vocab/standards"
    val rdf  = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

    "describe the variables" in {
      val sort = SortList(List(Sort(s"$base/createdAtTime"), Sort(s"-${rdf}type")))
      sort.toVars shouldEqual "?sort0 ?sort1"
      SortList.Empty.toVars shouldEqual ""
    }

    "define the triples" in {
      val sort = SortList(List(Sort(s"$base/createdAtTime"), Sort(s"-${rdf}type")))
      sort.toTriples shouldEqual
        s"""OPTIONAL{?s <$base/createdAtTime> ?sort0}
           |OPTIONAL{?s <${rdf}type> ?sort1}""".stripMargin
      SortList.Empty.toTriples shouldEqual ""

    }

    "set the ordering clause" in {
      val sort = SortList(List(Sort(s"$base/createdAtTime"), Sort(s"-${rdf}type")))
      sort.toOrderByClause shouldEqual "ASC(?sort0) DESC(?sort1)"
      SortList.Empty.toOrderByClause shouldEqual "?s"

    }

  }

}
