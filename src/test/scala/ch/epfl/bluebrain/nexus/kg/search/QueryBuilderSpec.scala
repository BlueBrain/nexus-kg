package ch.epfl.bluebrain.nexus.kg.search

import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.{Matchers, WordSpecLike}

class QueryBuilderSpec extends WordSpecLike with Matchers with Resources {

  "QueryBuilder" should {
    val schema: AbsoluteIri = url"http://nexus.example.com/testSchema".value
    "build query with deprecation" in {
      val expected = jsonContentOf("/search/query-deprecation.json")
      QueryBuilder.queryFor(Some(true)) shouldEqual expected
    }
    "build query without deprecation" in {
      val expected = jsonContentOf("/search/query-no-deprecation.json")
      QueryBuilder.queryFor(None) shouldEqual expected
    }
    "build query with schema and deprecation" in {
      val expected = jsonContentOf("/search/query-schema-deprecation.json")
      QueryBuilder.queryFor(Some(true), Some(schema)) shouldEqual expected

    }

    "build query with schema and without deprecation" in {
      val expected = jsonContentOf("/search/query-schema-no-deprecation.json")
      QueryBuilder.queryFor(None, Some(schema)) shouldEqual expected
    }
  }

}
