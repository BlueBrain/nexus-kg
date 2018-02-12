package ch.epfl.bluebrain.nexus.kg.query.schemas

import java.util.regex.Pattern.quote

import akka.http.scaladsl.client.RequestBuilding.Post
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.query.QueryFixture
import io.circe.{Decoder, Json}

class SchemasElasticQueriesSpec extends QueryFixture[SchemaId] {
  override implicit lazy val idDecoder: Decoder[SchemaId] = elasticIdDecoder

  val schemaQueries = SchemasElasticQueries(elasticClient, settings)
  val schemas: Map[(SchemaId, Boolean, Boolean), Json] = (for {
    orgCount <- 1 to 2
    orgName = genId()
    domCount <- 1 to 2
    domName = genId()
    schemaCount <- 1 to objectCount
    schemaName = genId()
    schemaVersion <- List(Version(0, 0, 1), Version(0, 0, 2))
    domId      = DomainId(OrgId(orgName), domName)
    schemaId   = SchemaId(domId, schemaName, schemaVersion)
    deprecated = math.random < 0.5
    published  = math.random < 0.5
    schemaJson = jsonContentOf(
      "/schemas/schema_source.json",
      Map(
        quote("{{orgName}}")    -> orgName,
        quote("{{domainName}}") -> domName,
        quote("{{schemaName}}") -> schemaName,
        quote("{{version}}")    -> schemaVersion.show,
        quote("{{deprecated}}") -> deprecated.toString,
        quote("{{published}}")  -> published.toString
      )
    )
  } yield (schemaId, deprecated, published) -> schemaJson).toMap
  implicit val ord: Ordering[SchemaId] = (x: SchemaId, y: SchemaId) => x.show.compare(y.show)
  val schemaIds: List[SchemaId]        = schemas.keys.map(_._1).toList.sorted

  override def beforeAll(): Unit = {
    super.beforeAll()
    elasticClient.createIndex("schemas", mapping).futureValue
    schemas.foreach {
      case ((schemaId, _, _), schemaJson) =>
        elasticClient.create("schemas", "doc", s"schemaid_${schemaId.show}", schemaJson).futureValue
    }
    val _ = untypedHttpClient(Post(refreshUri)).futureValue
  }

  "SchemasElasticQueries" should {
    "list all schemas with pagination" in {
      var i = 0L
      schemaIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = schemaQueries.list(Pagination(i, pageSize), None, None).futureValue
        results.total shouldEqual schemas.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list all schemas for organization with pagination" in {
      val schemasByOrgId: Map[OrgId, List[SchemaId]] = schemaIds.groupBy(_.domainId.orgId)
      schemasByOrgId.foreach {
        case (org, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), org, None, None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all schemas for domain with pagination" in {
      val schemasByDomainId: Map[DomainId, List[SchemaId]] = schemaIds.groupBy(_.domainId)
      schemasByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), dom, None, None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all deprecated schemas for domain with pagination" in {
      val schemasByDomainId: Map[DomainId, List[SchemaId]] =
        schemas.keys.toList.filter(_._2).map(_._1).sorted.groupBy(_.domainId)
      schemasByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), dom, Some(true), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all published schemas for domain with pagination" in {
      val schemasByDomainId: Map[DomainId, List[SchemaId]] =
        schemas.keys.toList.filter(_._3).map(_._1).sorted.groupBy(_.domainId)

      schemasByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), dom, None, Some(true)).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all non deprecated schemas for domain with pagination" in {
      val schemasByDomainId: Map[DomainId, List[SchemaId]] =
        schemas.keys.toList.filterNot(_._2).map(_._1).sorted.groupBy(_.domainId)
      schemasByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), dom, Some(false), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list non published schemas for domain with pagination" in {
      val schemasByDomainId: Map[DomainId, List[SchemaId]] =
        schemas.keys.toList.filterNot(_._3).map(_._1).sorted.groupBy(_.domainId)

      schemasByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), dom, None, Some(false)).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all schemas for schema group with pagination" in {
      val schemasBySchemaName: Map[SchemaName, List[SchemaId]] = schemaIds.groupBy(_.schemaName)
      schemasBySchemaName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), name, None, None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all published schemas for schema group with pagination" in {
      val schemasBySchemaName: Map[SchemaName, List[SchemaId]] =
        schemas.keys.toList.filter(_._3).map(_._1).sorted.groupBy(_.schemaName)
      schemasBySchemaName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), name, None, Some(true)).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all deprecated schemas for schema group with pagination" in {
      val schemasBySchemaName: Map[SchemaName, List[SchemaId]] =
        schemas.keys.toList.filter(_._2).map(_._1).sorted.groupBy(_.schemaName)
      schemasBySchemaName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), name, Some(true), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all non deprecated schemas for schema group with pagination" in {
      val schemasBySchemaName: Map[SchemaName, List[SchemaId]] =
        schemas.keys.toList.filterNot(_._2).map(_._1).sorted.groupBy(_.schemaName)
      schemasBySchemaName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), name, Some(false), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all non published for schema group with pagination" in {
      val schemasBySchemaName: Map[SchemaName, List[SchemaId]] =
        schemas.keys.toList.filterNot(_._3).map(_._1).sorted.groupBy(_.schemaName)
      schemasBySchemaName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = schemaQueries.list(Pagination(i, pageSize), name, None, Some(false)).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }
  }
}
