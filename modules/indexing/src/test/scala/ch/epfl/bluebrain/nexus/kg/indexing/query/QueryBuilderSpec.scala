package ch.epfl.bluebrain.nexus.kg.indexing.query

import akka.http.scaladsl.model.Uri
import cats.instances.string._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilder.SelectField._
import ch.epfl.bluebrain.nexus.kg.indexing.query.QueryBuilder.WhereField._
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import org.apache.jena.query.QueryParseException
import org.scalatest.{Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.kg.indexing.query.IndexingVocab.SelectTerms._

class QueryBuilderSpec extends WordSpecLike with Matchers {

  "A QueryBuilder" should {
    val vocab = Uri(s"http://localhost/v0/voc/")
    val qualifyVocab = Uri(s"http://localhost/v0/voc")
    implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](qualifyVocab)

    "create a valid select and filter query" in {
      val expected =
        s"""PREFIX  vocab: <${vocab.toString}>
           |
           |SELECT  ?s ?org ?domain ?name
           |WHERE
           |  { ?s  vocab:organization  ?org ;
           |        vocab:domain        ?domain ;
           |        vocab:version       ?v ;
           |        vocab:name          ?name
           |    FILTER ( ( ( ( ?org = "bbp" ) && ( ?domain = "experiment" ) ) && ( ?name = "one" ) ) && ( ?v = 2 ) )
           |  }
        """.stripMargin.trim

      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject, "org", "domain", "name")
        .where("vocab:organization" -> "org")
        .where("vocab:domain" -> "domain")
        .where("vocab:version" -> "v")
        .where("vocab:name" -> "name")
        .filter("""?org = "bbp"  && ?domain = "experiment" && ?name = "one" && ?v = 2""").build().toString.trim shouldEqual expected
    }

    "create a query with a subquery" in {

      val expected =
        s"""
           |PREFIX  vocab: <${vocab.toString}>
           |
           |SELECT  ?s ?org ?domain ?name
           |WHERE
           |  { { SELECT  ?s
           |      WHERE
           |        { ?s  vocab:organization  ?org
           |          FILTER ( ?org = "bbp" )
           |        }
           |    }
           |    ?s  vocab:domain   ?domain ;
           |        vocab:version  ?v ;
           |        vocab:name     ?name
           |    FILTER ( ( ( ?domain = "experiment" ) && ( ?name = "one" ) ) && ( ?v = 2 ) )
           |  }""".stripMargin.trim

      val subQuery = QueryBuilder.select(subject)
        .where("organization".qualify -> "org")
        .filter("""?org = "bbp"""")

      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject, "org", "domain", "name")
        .subQuery(subQuery)
        .where("vocab:domain" -> "domain")
        .where("vocab:version" -> "v")
        .where("vocab:name" -> "name")
        .filter("""?domain = "experiment" && ?name = "one" && ?v = 2""").build().toString().trim shouldEqual expected
    }

    "create a full text search query" in {
      val vocab = Uri("http://www.bigdata.com/rdf/search#")
      val term = "rand"
      val expected =
        s"""
           |PREFIX  bds:  <${vocab}>
           |
           |SELECT  ?s ?matchedProperty ?score ?rank (GROUP_CONCAT(DISTINCT ?matchedValue ; separator=',') AS ?groupedConcatenatedMatchedValue)
           |WHERE
           |  { ?matchedValue
           |              bds:search        "$term" ;
           |              bds:relevance     ?score ;
           |              bds:rank          ?rank ;
           |              ?matchedProperty  ?matchedValue
           |    FILTER ( ! isBlank(?s) )
           |  }
         """.stripMargin.trim

      QueryBuilder.prefix("bds" -> Uri("http://www.bigdata.com/rdf/search#"))
        .select(subject, "matchedProperty", score, rank, "GROUP_CONCAT(DISTINCT ?matchedValue ; separator=',')" -> "groupedConcatenatedMatchedValue")
        .where((WhereVar("matchedValue"), WhereVal("bds:search"), WhereVal(term)))
        .where((WhereVar("matchedValue"), WhereVal("bds:relevance"), WhereVar("score")))
        .where((WhereVar("matchedValue"), WhereVal("bds:rank"), WhereVar("rank")))
        .where((WhereVar("matchedValue"), WhereVar("matchedProperty"), WhereVar("matchedValue")))
        .filter("""!isBlank(?s)""").build().toString().trim shouldEqual expected

    }

    "create a query with a subquery and union" in {
      val expected =
        s"""PREFIX  vocab: <${vocab}>
           |
           |SELECT  ?org ?s
           |WHERE
           |  {   { SELECT  ?org
           |        WHERE
           |          { ?s  vocab:organization  ?org
           |            FILTER ( ?org = "bbp" )
           |          }
           |      }
           |    UNION
           |      { SELECT  ?s
           |        WHERE
           |          { ?s  vocab:domain  ?domain
           |            FILTER ( ?domain = "core" )
           |          }
           |      }}
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
        .union(subQuery2).build().toString().trim shouldEqual expected
    }

    "create a valid select and filter and groupBy query" in {
      val expected =
        s"""PREFIX  vocab: <${vocab.toString}>
           |
           |SELECT  ?s ?org ?domain ?name
           |WHERE
           |  { ?s  vocab:organization  ?org ;
           |        vocab:domain        ?domain ;
           |        vocab:version       ?v ;
           |        vocab:name          ?name
           |    FILTER ( ( ( ( ?org = "bbp" ) && ( ?domain = "experiment" ) ) && ( ?name = "one" ) ) && ( ?v = 2 ) )
           |  }
           |GROUP BY ?s ?org ?domain ?name
        """.stripMargin.trim

      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject, "org", "domain", "name")
        .where("vocab:organization" -> "org")
        .where("vocab:domain" -> "domain")
        .where("vocab:version" -> "v")
        .where("vocab:name" -> "name")
        .filter("""?org = "bbp"  && ?domain = "experiment" && ?name = "one" && ?v = 2""")
        .groupBy(subject, "org", "domain", "name").build().toString.trim shouldEqual expected
    }

    "create a valid select and filter and groupBy query without prefix" in {
      val expected =
        s"""SELECT  ?s ?org ?domain ?name
           |WHERE
           |  { ?s  <$qualifyVocab/organization>  ?org ;
           |        <$qualifyVocab/domain>  ?domain ;
           |        <$qualifyVocab/version>  ?v ;
           |        <$qualifyVocab/name>  ?name
           |    FILTER ( ( ( ( ?org = "bbp" ) && ( ?domain = "experiment" ) ) && ( ?name = "one" ) ) && ( ?v = 2 ) )
           |  }
           |GROUP BY ?s ?org ?domain ?name
        """.stripMargin.trim

      QueryBuilder.select(subject, "org", "domain", "name")
        .where("organization".qualify -> "org")
        .where("domain".qualify -> "domain")
        .where("version".qualify -> "v")
        .where("name".qualify -> "name")
        .filter("""?org = "bbp"  && ?domain = "experiment" && ?name = "one" && ?v = 2""")
        .groupBy(subject, "org", "domain", "name").build().toString.trim shouldEqual expected
    }

    "create a query builder with the correct offset" in {
      val expected =
        s"""SELECT  ?s ?org ?domain ?name
           |WHERE
           |  { ?s  <$qualifyVocab/organization>  ?org}
           |OFFSET  3
           |LIMIT   3
        """.stripMargin.trim

      QueryBuilder.select(subject, "org", "domain", "name")
        .where("organization".qualify -> "org")
        .pagination(Pagination(3L,3)).build().toString.trim shouldEqual expected
    }

    "create a query with wrong filter" in {
      val builder = QueryBuilder.prefix("vocab" -> vocab)
        .select(subject, "org", "domain", "name")
        .where("vocab:organization" -> "org")
        .where("vocab:domain" -> "domain")
        .where("vocab:version" -> "v")
        .where("vocab:name" -> "name")
        .filter("""?org >!= "bbp"  && ?domain = "experiment"""")
      an[QueryParseException] should be thrownBy builder.build()
    }

    "create a query without filter" in {
      val expected =
        s"""PREFIX  vocab: <${vocab.toString}>
           |
           |SELECT  ?s
           |WHERE
           |  { ?s  vocab:organization  ?org ;
           |        vocab:domain        ?domain ;
           |        vocab:version       ?v ;
           |        vocab:name          ?name}
        """.stripMargin.trim

      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject)
        .where("vocab:organization" -> "org")
        .where("vocab:domain" -> "domain")
        .where("vocab:version" -> "v")
        .where("vocab:name" -> "name").build().toString.trim shouldEqual expected
    }

    "create a query with select distinct" in {
      val expected =
        s"""PREFIX  vocab: <${vocab.toString}>
           |
           |SELECT DISTINCT  ?s
           |WHERE
           |  { ?s  vocab:organization  ?org ;
           |        vocab:domain        ?domain ;
           |        vocab:version       ?v ;
           |        vocab:name          ?name}
        """.stripMargin.trim

      QueryBuilder
        .prefix("vocab" -> vocab)
        .selectDistinct(subject)
        .where("vocab:organization" -> "org")
        .where("vocab:domain" -> "domain")
        .where("vocab:version" -> "v")
        .where("vocab:name" -> "name").build().toString.trim shouldEqual expected
    }


    "create a query counting the total (sparQL compatible)" in {

      val expected =
        s"""
           |PREFIX  vocab: <${vocab.toString}>
           |
           |SELECT  ?total ?s
           |WHERE
           |  {   { SELECT  (COUNT(?s) AS ?total)
           |        WHERE
           |          { ?s  vocab:organization  ?org ;
           |                vocab:domain        ?domain ;
           |                vocab:version       ?v ;
           |                vocab:schema        ?name
           |            FILTER ( ( ?org = "bbp" ) && ( ?domain = "core" ) )
           |          }
           |      }
           |    UNION
           |      { SELECT  ?s
           |        WHERE
           |          { ?s  vocab:organization  ?org ;
           |                vocab:domain        ?domain ;
           |                vocab:version       ?v ;
           |                vocab:schema        ?name
           |            FILTER ( ( ?org = "bbp" ) && ( ?domain = "core" ) )
           |          }
           |        GROUP BY ?total ?s
           |        LIMIT   10
           |      }}
         """.stripMargin.trim

      QueryBuilder.prefix("vocab" -> vocab)
        .select(subject)
        .where("vocab:organization" -> "org")
        .where("vocab:domain" -> "domain")
        .where("vocab:version" -> "v")
        .where("vocab:schema" -> "name")
        .filter("""?org = "bbp" && ?domain = "core"""")
        .pagination(Pagination(0L, 10))
        .groupBy(total, subject)
        .total(subject -> total).build().serialize().trim shouldEqual expected
    }
  }
}
