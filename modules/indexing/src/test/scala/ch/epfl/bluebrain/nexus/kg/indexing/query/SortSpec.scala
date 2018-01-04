package ch.epfl.bluebrain.nexus.kg.indexing.query

import org.scalatest.{Matchers, WordSpecLike}

class SortSpec extends WordSpecLike with Matchers {

  "A Sort" should {
    "reject when empty" in {
      Sort("") shouldEqual None
    }

    "reject when not absolute URI " in {
      Sort("something") shouldEqual None
    }
  }

  "A SortList" should {
    val base = "http://localhost/vocab/standards"
    val rdf  = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

    "describe the variables" in {
      val sort = SortList(List(Sort(s"$base/createdAtTime").get, Sort(s"-${rdf}type").get))
      sort.toVars shouldEqual "?sort0 ?sort1"
      SortList.Empty.toVars shouldEqual ""
    }

    "define the triples" in {
      val sort = SortList(List(Sort(s"$base/createdAtTime").get, Sort(s"-${rdf}type").get))
      sort.toTriples shouldEqual
        s"""OPTIONAL{?s <$base/createdAtTime> ?sort0}
           |OPTIONAL{?s <${rdf}type> ?sort1}""".stripMargin
      SortList.Empty.toTriples shouldEqual ""

    }

    "set the ordering clause" in {
      val sort = SortList(List(Sort(s"$base/createdAtTime").get, Sort(s"-${rdf}type").get))
      sort.toOrderByClause shouldEqual "ASC(?sort0) DESC(?sort1)"
      SortList.Empty.toOrderByClause shouldEqual "?s"

    }

  }

}
