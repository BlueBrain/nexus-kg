package ch.epfl.bluebrain.nexus.kg.query.instances

import java.util.UUID
import java.util.regex.Pattern.quote

import akka.http.scaladsl.client.RequestBuilding.Post
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.query.QueryFixture
import io.circe.{Decoder, Json}

class InstancesElasticQueriesSpec extends QueryFixture[InstanceId] {
  override implicit lazy val idDecoder: Decoder[InstanceId] = elasticIdDecoder

  val instanceQueries = InstancesElasticQueries(elasticClient, settings)
  val instances: Map[(InstanceId, Boolean), Json] = (for {
    orgCount <- 1 to 2
    orgName = genId()
    domCount <- 1 to 2
    domName = genId()
    schemaCount <- 1 to 2
    schemaName = genId()
    domId      = DomainId(OrgId(orgName), domName)
    schemaVersion <- List(Version(0, 0, 1), Version(0, 0, 2))
    schemaId = SchemaId(domId, schemaName, schemaVersion)
    instanceCount <- 1 to objectCount
    instanceId = InstanceId(schemaId, UUID.randomUUID().toString)
    deprecated = math.random < 0.5
    instanceJson = jsonContentOf(
      "/instances/instance_source.json",
      Map(
        quote("{{orgName}}")       -> orgName,
        quote("{{domainName}}")    -> domName,
        quote("{{schemaName}}")    -> schemaName,
        quote("{{schemaVersion}}") -> schemaVersion.show,
        quote("{{instanceId}}")    -> instanceId.id,
        quote("{{deprecated}}")    -> deprecated.toString
      )
    )
  } yield (instanceId, deprecated) -> instanceJson).toMap
  implicit val ord: Ordering[InstanceId] = (x: InstanceId, y: InstanceId) => x.show.compare(y.show)
  val instanceIds: List[InstanceId]      = instances.keys.toList.map(_._1).sorted
  val deprecatedIds: List[InstanceId]    = instances.keys.toList.filter(_._2).map(_._1).sorted
  val nonDeprecatedIds: List[InstanceId] = instances.keys.toList.filterNot(_._2).map(_._1).sorted

  override def beforeAll(): Unit = {
    super.beforeAll()
    elasticClient.createIndex("instances", mapping).futureValue
    instances.foreach {
      case ((instanceId, _), instanceJson) =>
        elasticClient.create("instances", "doc", s"instanceid_${instanceId.show}", instanceJson).futureValue
    }
    val _ = untypedHttpClient(Post(refreshUri)).futureValue
  }

  "InstancesElasticQueries" should {
    "list all instances with pagination" in {
      var i = 0L
      instanceIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = instanceQueries.list(Pagination(i, pageSize), None, None).futureValue
        results.total shouldEqual instances.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list all deprecated instances with pagination" in {
      var i = 0L
      deprecatedIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = instanceQueries.list(Pagination(i, pageSize), Some(true), None).futureValue
        results.total shouldEqual deprecatedIds.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list all non deprecated instances with pagination" in {
      var i = 0L
      nonDeprecatedIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = instanceQueries.list(Pagination(i, pageSize), Some(false), None).futureValue
        results.total shouldEqual nonDeprecatedIds.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list all instances for organization with pagination" in {
      val instancesByOrgId: Map[OrgId, List[InstanceId]] = instanceIds.groupBy(_.schemaId.domainId.orgId)
      instancesByOrgId.foreach {
        case (org, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), org, None, None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all deprecated instances for organization with pagination" in {
      val instancesByOrgId: Map[OrgId, List[InstanceId]] = deprecatedIds.groupBy(_.schemaId.domainId.orgId)
      instancesByOrgId.foreach {
        case (org, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), org, Some(true), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all non deprecated instances for organization with pagination" in {
      val instancesByOrgId: Map[OrgId, List[InstanceId]] = nonDeprecatedIds.groupBy(_.schemaId.domainId.orgId)
      instancesByOrgId.foreach {
        case (org, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), org, Some(false), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all instances for domain with pagination" in {
      val instancesByDomainId: Map[DomainId, List[InstanceId]] = instanceIds.groupBy(_.schemaId.domainId)
      instancesByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), dom, None, None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all deprecated instances for domain with pagination" in {
      val instancesByDomainId: Map[DomainId, List[InstanceId]] = deprecatedIds.groupBy(_.schemaId.domainId)
      instancesByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), dom, Some(true), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all non deprecated instances for domain with pagination" in {
      val instancesByDomainId: Map[DomainId, List[InstanceId]] = nonDeprecatedIds.groupBy(_.schemaId.domainId)
      instancesByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), dom, Some(false), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all instances for schema group with pagination" in {
      val instancesBySchemaName: Map[SchemaName, List[InstanceId]] = instanceIds.groupBy(_.schemaId.schemaName)
      instancesBySchemaName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), name, None, None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all deprecated instances for schema group with pagination" in {
      val instancesBySchemaName: Map[SchemaName, List[InstanceId]] = deprecatedIds.groupBy(_.schemaId.schemaName)
      instancesBySchemaName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), name, Some(true), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all non deprecated instances for schema group with pagination" in {
      val instancesBySchemaName: Map[SchemaName, List[InstanceId]] = nonDeprecatedIds.groupBy(_.schemaId.schemaName)
      instancesBySchemaName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), name, Some(false), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all instances for schema  with pagination" in {
      val instancesBySchemaName: Map[SchemaId, List[InstanceId]] = instanceIds.groupBy(_.schemaId)
      instancesBySchemaName.foreach {
        case (schemaId, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), schemaId, None, None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all deprecated  instances for schema  with pagination" in {
      val instancesBySchemaName: Map[SchemaId, List[InstanceId]] = deprecatedIds.groupBy(_.schemaId)
      instancesBySchemaName.foreach {
        case (schemaId, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), schemaId, Some(true), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all non deprecated instances for schema  with pagination" in {
      val instancesBySchemaName: Map[SchemaId, List[InstanceId]] = nonDeprecatedIds.groupBy(_.schemaId)
      instancesBySchemaName.foreach {
        case (schemaId, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = instanceQueries.list(Pagination(i, pageSize), schemaId, Some(false), None).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }
  }
}
