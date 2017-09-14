package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import akka.http.scaladsl.model.Uri
import cats.instances.string._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.PrefixUri._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Field._
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.Order.Desc
import ch.epfl.bluebrain.nexus.kg.indexing.query.builder.QueryBuilder.TripleContent._
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import org.scalatest.{Matchers, WordSpecLike}

class QueryBuilderSpec extends WordSpecLike with Matchers {

  "A QueryBuilder" should {
    val vocab = Uri(s"http://localhost/v0/voc/")
    val qualifyVocab = Uri(s"http://localhost/v0/voc")
    implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](qualifyVocab)

    "create a valid select and filter query" in {
      val expected =
        s"""PREFIX vocab: <${vocab.toString}>
           |SELECT ?s ?org ?domain ?name
           |WHERE
           |{
           |	?s vocab:organization ?org .
           |	?s vocab:domain ?domain .
           |	?s vocab:version ?v .
           |	?s vocab:name ?name .
           |	FILTER ( ?org = "bbp" && ?domain = "experiment" && ?name = "one" && ?v = 2 )
           |}
        """.stripMargin.trim

      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject, "org", "domain", "name")
        .where(QueryContent("vocab:organization") -> "org")
        .where(QueryContent("vocab:domain") -> "domain")
        .where(QueryContent("vocab:version") -> "v")
        .where(QueryContent("vocab:name") -> "name")
        .filter("""?org = "bbp" && ?domain = "experiment" && ?name = "one" && ?v = 2""")
        .build().pretty shouldEqual expected
    }

    "create a query with a subquery" in {


      val expected =
        s"""
           |PREFIX vocab: <${vocab.toString}>
           |SELECT ?s ?org ?domain ?name
           |WHERE
           |{
           |	{ SELECT ?s
           |		WHERE
           |		{
           |			?s <http://localhost/v0/voc/organization> ?org .
           |			FILTER ( ?org = "bbp" )
           |		}
           |	}
           |	?s vocab:domain ?domain .
           |	?s vocab:version ?v .
           |	?s vocab:name ?name .
           |	FILTER ( ?domain = "experiment" && ?name = "one" && ?v = 2 )
           |}
         """.stripMargin.trim

      val subQuery = QueryBuilder.select(subject)
        .where("organization".qualify -> "org")
        .filter("""?org = "bbp"""")


      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject, "org", "domain", "name")
        .subQuery(subQuery)
        .where(QueryContent("vocab:domain") -> "domain")
        .where(QueryContent("vocab:version") -> "v")
        .where(QueryContent("vocab:name") -> "name")
        .filter("""?domain = "experiment" && ?name = "one" && ?v = 2""")
        .build().pretty shouldEqual expected
    }

    "create a full text search query" in {
      val vocab = Uri("http://www.bigdata.com/rdf/search#")
      val term = "rand"
      val expected =
        s"""
           |PREFIX bds: <${vocab}>
           |SELECT ?s ?matchedProperty ?score ?rank (GROUP_CONCAT(DISTINCT ?matchedValue ; separator=',') AS ?groupedConcatenatedMatchedValue)
           |WHERE
           |{
           |	?matchedValue bds:search "$term" .
           |	?matchedValue bds:relevance ?score .
           |	?matchedValue bds:rank ?rank .
           |	?matchedValue ?matchedProperty ?matchedValue .
           |	FILTER ( ! isBlank(?s) )
           |}
         """.stripMargin.trim

      QueryBuilder.prefix("bds" -> Uri("http://www.bigdata.com/rdf/search#"))
        .select(subject, "matchedProperty", score, rank, "GROUP_CONCAT(DISTINCT ?matchedValue ; separator=',')" -> "groupedConcatenatedMatchedValue")
        .where((VarContent("matchedValue"), QueryContent("bds:search"), ValContent(term)))
        .where((VarContent("matchedValue"), QueryContent("bds:relevance"), VarContent("score")))
        .where((VarContent("matchedValue"), QueryContent("bds:rank"), VarContent("rank")))
        .where((VarContent("matchedValue"), VarContent("matchedProperty"), VarContent("matchedValue")))
        .filter("""! isBlank(?s)""")
        .build().pretty shouldEqual expected

    }

    "create a query with a subquery and union" in {
      val expected =
        s"""PREFIX vocab: <${vocab}>
           |SELECT ?org ?s
           |WHERE
           |{
           |	{ SELECT ?org
           |		WHERE
           |		{
           |			?s <http://localhost/v0/voc/organization> ?org .
           |			FILTER ( ?org = "bbp" )
           |		}
           |	}
           |	UNION
           |	{
           |		SELECT ?s
           |		WHERE
           |		{
           |			?s <http://localhost/v0/voc/domain> ?domain .
           |			FILTER ( ?domain = "core" )
           |		}
           |	}
           |}
         """.stripMargin.trim
      val subQuery1 = QueryBuilder.select("org")
        .where("organization".qualify -> "org")
        .filter("""?org = "bbp"""")

      val subQuery2 = QueryBuilder.select(subject)
        .where("domain".qualify -> "domain")
        .filter("""?domain = "core"""")

      QueryBuilder.prefix("vocab" -> vocab)
        .select("org", subject)
        .subQuery(subQuery1)
        .union(subQuery2)
        .build().pretty shouldEqual expected
    }

    "create a valid select and filter and groupBy query" in {
      val expected =
        s"""PREFIX vocab: <${vocab.toString}>
           |SELECT ?s ?org ?domain ?name
           |WHERE
           |{
           |	?s vocab:organization ?org .
           |	?s vocab:domain ?domain .
           |	?s vocab:version ?v .
           |	?s vocab:name ?name .
           |	FILTER ( ?org = "bbp"  && ?domain = "experiment" && ?name = "one" && ?v = 2 )
           |}
           |GROUP BY ?s ?org ?domain ?name
        """.stripMargin.trim

      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject, "org", "domain", "name")
        .where(QueryContent("vocab:organization") -> "org")
        .where(QueryContent("vocab:domain") -> "domain")
        .where(QueryContent("vocab:version") -> "v")
        .where(QueryContent("vocab:name") -> "name")
        .filter("""?org = "bbp"  && ?domain = "experiment" && ?name = "one" && ?v = 2""")
        .groupBy(subject, "org", "domain", "name")
        .build().pretty shouldEqual expected
    }

    "create a valid select and filter and groupBy query without prefix" in {
      val expected =
        s"""SELECT ?s ?org ?domain ?name
           |WHERE
           |{
           |	?s <$qualifyVocab/organization> ?org .
           |	?s <$qualifyVocab/domain> ?domain .
           |	?s <$qualifyVocab/version> ?v .
           |	?s <$qualifyVocab/name> ?name .
           |	FILTER ( ?org = "bbp"  && ?domain = "experiment" && ?name = "one" && ?v = 2 )
           |}
           |GROUP BY ?s ?org ?domain ?name
        """.stripMargin.trim

      QueryBuilder.select(subject, "org", "domain", "name")
        .where("organization".qualify -> "org")
        .where("domain".qualify -> "domain")
        .where("version".qualify -> "v")
        .where("name".qualify -> "name")
        .filter("""?org = "bbp"  && ?domain = "experiment" && ?name = "one" && ?v = 2""")
        .groupBy(subject, "org", "domain", "name")
        .build().pretty shouldEqual expected
    }

    "create a query builder with the correct offset" in {
      val expected =
        s"""SELECT ?s ?org ?domain ?name
           |WHERE
           |{
           |	?s <$qualifyVocab/organization> ?org .
           |}
           |LIMIT 3
           |OFFSET 3
        """.stripMargin.trim

      QueryBuilder.select(subject, "org", "domain", "name")
        .where("organization".qualify -> "org")
        .pagination(Pagination(3L, 3))
        .build().pretty shouldEqual expected
    }

    "create a query without filter" in {
      val expected =
        s"""PREFIX vocab: <${vocab.toString}>
           |SELECT ?s
           |WHERE
           |{
           |	?s vocab:organization ?org .
           |	?s vocab:domain ?domain .
           |	?s vocab:version ?v .
           |	?s vocab:name ?name .
           |}
        """.stripMargin.trim

      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject)
        .where(QueryContent("vocab:organization") -> "org")
        .where(QueryContent("vocab:domain") -> "domain")
        .where(QueryContent("vocab:version") -> "v")
        .where(QueryContent("vocab:name") -> "name")
        .build().pretty shouldEqual expected
    }

    "create a query with select distinct" in {
      val expected =
        s"""PREFIX vocab: <${vocab.toString}>
           |SELECT DISTINCT ?s
           |WHERE
           |{
           |	?s vocab:organization ?org .
           |	?s vocab:domain ?domain .
           |	?s vocab:version ?v .
           |	?s vocab:name ?name .
           |}
        """.stripMargin.trim

      QueryBuilder
        .prefix("vocab" -> vocab)
        .selectDistinct(subject)
        .where(QueryContent("vocab:organization") -> "org")
        .where(QueryContent("vocab:domain") -> "domain")
        .where(QueryContent("vocab:version") -> "v")
        .where(QueryContent("vocab:name") -> "name")
        .build().pretty shouldEqual expected
    }

    "create a query with two subQueries" in {
      val expected =
        s"""
           |PREFIX vocab: <${vocab.toString()}>
           |SELECT ?s
           |WHERE
           |{
           |	{ SELECT ?s
           |		WHERE
           |		{
           |			?s vocab:organization ?org .
           |		}
           |	}
           |	{ SELECT ?s
           |		WHERE
           |		{
           |			?s vocab:domain ?domain .
           |		}
           |	}
           |}
         """.stripMargin.trim
      QueryBuilder
        .prefix("vocab" -> vocab)
        .select(subject)
        .subQuery(QueryBuilder.select(subject).where(QueryContent("vocab:organization") -> "org"))
        .subQuery(QueryBuilder.select(subject).where(QueryContent("vocab:domain") -> "domain"))
        .build().pretty shouldEqual expected
    }

    "create a query with two unions" in {
      val expected =
        s"""
           |PREFIX vocab: <${vocab.toString()}>
           |SELECT ?s
           |WHERE
           |{
           |	{ ?s vocab:organization ?org .
           |	}
           |	UNION
           |	{
           |		SELECT ?s
           |		WHERE
           |		{
           |			?s vocab:name ?name .
           |		}
           |	}UNION
           |	{
           |		SELECT ?s
           |		WHERE
           |		{
           |			?s vocab:domain ?domain .
           |		}
           |	}
           |}
         """.stripMargin.trim
     
      QueryBuilder
        .prefix("vocab" -> vocab)
        .select(subject)
        .union(QueryBuilder.select(subject).where(QueryContent("vocab:name") -> "name"))
        .union(QueryBuilder.select(subject).where(QueryContent("vocab:domain") -> "domain"))
        .where(QueryContent("vocab:organization") -> "org")
        .build().pretty shouldEqual expected
    }


    "create a query counting the total" in {

      val expected =
        s"""
           |PREFIX vocab: <${vocab.toString()}>
           |SELECT ?total ?s
           |WITH {
           |	SELECT ?s
           |	WHERE
           |	{
           |		?s vocab:organization ?org .
           |		?s vocab:domain ?domain .
           |		?s vocab:version ?v .
           |		?s vocab:schema ?name .
           |		FILTER ( ?org = "bbp" && ?domain = "core" )
           |	}
           |	GROUP BY ?total ?s
           |} AS %resultSet
           |WHERE
           |{
           |	{ SELECT (COUNT(DISTINCT ?s) AS ?total)
           |		WHERE
           |		{
           |			INCLUDE %resultSet
           |		}
           |	}
           |	UNION
           |	{
           |		SELECT *
           |		WHERE
           |		{
           |			?s vocab:organization ?org .
           |			?s vocab:domain ?domain .
           |			?s vocab:version ?v .
           |			?s vocab:schema ?name .
           |			FILTER ( ?org = "bbp" && ?domain = "core" )
           |			INCLUDE %resultSet
           |		}
           |		GROUP BY ?total ?s
           |		LIMIT 10
           |	}
           |}
         """.stripMargin.trim


      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject)
        .where(QueryContent("vocab:organization") -> "org")
        .where(QueryContent("vocab:domain") -> "domain")
        .where(QueryContent("vocab:version") -> "v")
        .where(QueryContent("vocab:schema") -> "name")
        .filter("""?org = "bbp" && ?domain = "core"""")
        .pagination(Pagination(0L, 10))
        .groupBy(total, subject)
        .buildCount().pretty shouldEqual expected
    }


    "create a query with WITH, INCLUDE, OPTIONAL and ORDER BY clauses" in {
      val term = "subject*"
      val expected =
        s"""
           |PREFIX bds: <http://www.bigdata.com/rdf/search#>
           |SELECT DISTINCT ?total ?s ?maxscore ?score ?rank ?field
           |WITH {
           |	SELECT DISTINCT ?s (max(?rsv) AS ?score) (max(?pos) AS ?rank)
           |	WHERE
           |	{
           |		?s ?matchedProperty ?matchedValue .
           |		?matchedValue bds:search "subject*" .
           |		?matchedValue bds:relevance ?rsv .
           |		?matchedValue bds:rank ?pos .
           |	}
           |	GROUP BY ?s
           |} AS %resultSet
           |WHERE
           |{
           |	{ SELECT (COUNT(DISTINCT ?s) AS ?total) (max(?score) AS ?maxscore)
           |		WHERE
           |		{
           |			INCLUDE %resultSet
           |		}
           |	}
           |	{ SELECT DISTINCT ?field
           |		WHERE
           |		{
           |			INCLUDE %resultSet
           |			OPTIONAL {
           |				?s <http://localhost/v0/voc/organization> ?field .
           |			}
           |		}
           |	}
           |	INCLUDE %resultSet
           |}
           |ORDER BY DESC(?score) ASC(?s)
           |LIMIT 10
         """.stripMargin.trim

      val withQuery =
        QueryBuilder.selectDistinct(subject, "max(?rsv)" -> score, "max(?pos)" -> rank)
          .where((VarContent(subject), VarContent("matchedProperty"), VarContent("matchedValue")))
          .where((VarContent("matchedValue"), QueryContent(bdsSearch), ValContent(term)))
          .where((VarContent("matchedValue"), QueryContent(bdsRelevance), VarContent("rsv")))
          .where((VarContent("matchedValue"), QueryContent(bdsRank), VarContent("pos")))
          .groupBy(subject)

      QueryBuilder.prefix("bds" -> bdsUri)
        .selectDistinct(total, subject, maxScore, score, rank, "field")
        .`with`(withQuery, "resultSet")
        .subQuery(QueryBuilder.select(count(), s"max(?$score)" -> maxScore).include("resultSet"))
        .subQuery(QueryBuilder.selectDistinct("field").optional("organization".qualify -> "field").include("resultSet"))
        .include("resultSet")
        .pagination(Pagination(0L, 10))
        .orderBy(Desc(score)).orderBy(subject)
        .build().pretty shouldEqual expected
    }
  }
}
