package ch.epfl.bluebrain.nexus.kg.query.instances

import java.util.UUID
import java.util.regex.Pattern.quote

import akka.http.scaladsl.client.RequestBuilding.Post
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.Read
import ch.epfl.bluebrain.nexus.commons.iam.acls.{FullAccessControlList, Path, Permissions}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
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
    val domains = instances.keys.map(_._1).groupBy(_.schemaId.domainId).mapValues(_.head).values
    domains.foreach(id => elasticClient.createIndex(id.toIndex(elasticPrefix), mapping).futureValue)
    instances.foreach {
      case ((instanceId, _), instanceJson) =>
        elasticClient.create(instanceId.toIndex(elasticPrefix), "doc", instanceId.elasticId, instanceJson).futureValue
    }
    val _ = untypedHttpClient(Post(refreshUri)).futureValue
  }

  "InstancesElasticQueries" should {
    "list all instances with pagination" in {
      var i = 0L
      instanceIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = instanceQueries.list(Pagination(i, pageSize), None, None, defaultAcls).futureValue
        results.total shouldEqual instances.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list all deprecated instances with pagination" in {
      var i = 0L
      deprecatedIds.sliding(pageSize, pageSize).foreach { ids =>
        val results =
          instanceQueries.list(Pagination(i, pageSize), Some(true), None, defaultAcls).futureValue
        results.total shouldEqual deprecatedIds.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list all non deprecated instances with pagination" in {
      var i = 0L
      nonDeprecatedIds.sliding(pageSize, pageSize).foreach { ids =>
        val results =
          instanceQueries.list(Pagination(i, pageSize), Some(false), None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), org, None, None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), org, Some(true), None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), org, Some(false), None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), dom, None, None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), dom, Some(true), None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), dom, Some(false), None, defaultAcls).futureValue
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
            val results = instanceQueries.list(Pagination(i, pageSize), name, None, None, defaultAcls).futureValue
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
            val results = instanceQueries.list(Pagination(i, pageSize), name, Some(true), None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), name, Some(false), None, defaultAcls).futureValue
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
            val results = instanceQueries.list(Pagination(i, pageSize), schemaId, None, None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), schemaId, Some(true), None, defaultAcls).futureValue
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
            val results =
              instanceQueries.list(Pagination(i, pageSize), schemaId, Some(false), None, defaultAcls).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "return 0 instances when user has no permissions " in {
      val results = instanceQueries.list(Pagination(0, pageSize), None, None, FullAccessControlList()).futureValue
      results.total shouldEqual 0
      results.results.size shouldEqual 0
    }

    "return only instances for organization the user has permissions for" in {
      val orgId: OrgId  = instanceIds.head.schemaId.domainId.orgId
      val acls          = FullAccessControlList((Identity.Anonymous(), Path(s"/kg/${orgId.show}"), Permissions(Read)))
      val expectedTotal = instanceIds.count(_.schemaId.domainId.orgId == orgId)
      val results       = instanceQueries.list(Pagination(0, pageSize), None, None, acls).futureValue
      results.total shouldEqual expectedTotal
    }

    "return only instances for domain the user has permissions for" in {
      val domId: DomainId = instanceIds.head.schemaId.domainId
      val acls            = FullAccessControlList((Identity.Anonymous(), Path(s"/kg/${domId.show}"), Permissions(Read)))
      val expectedTotal   = instanceIds.count(_.schemaId.domainId == domId)
      val results         = instanceQueries.list(Pagination(0, pageSize), None, None, acls).futureValue
      results.total shouldEqual expectedTotal
    }

    "return only instances for domain and organization the user has permissions for" in {
      val orgId: OrgId    = instanceIds.head.schemaId.domainId.orgId
      val domId: DomainId = instanceIds.last.schemaId.domainId
      val acls = FullAccessControlList(
        (Identity.Anonymous(), Path(s"/kg/${domId.show}"), Permissions(Read)),
        (Identity.Anonymous(), Path(s"/kg/${orgId.show}"), Permissions(Read))
      )
      val expectedTotal = instanceIds.count(instanceId =>
        instanceId.schemaId.domainId == domId || instanceId.schemaId.domainId.orgId == orgId)
      val results = instanceQueries.list(Pagination(0, pageSize), None, None, acls).futureValue
      results.total shouldEqual expectedTotal
    }

    "return 0 if the user has no permissions for a given organization" in {
      val orgId: OrgId  = instanceIds.head.schemaId.domainId.orgId
      val orgId2: OrgId = instanceIds.last.schemaId.domainId.orgId
      val acls = FullAccessControlList(
        (Identity.Anonymous(), Path(s"/kg/${orgId2.show}"), Permissions(Read))
      )
      val results = instanceQueries.list(Pagination(0, pageSize), orgId, None, None, acls).futureValue
      results.total shouldEqual 0
    }

    "return 0 if the user has no permissions for a given domain" in {
      val domainId: DomainId  = instanceIds.head.schemaId.domainId
      val domainId2: DomainId = instanceIds.last.schemaId.domainId
      val acls = FullAccessControlList(
        (Identity.Anonymous(), Path(s"/kg/${domainId2.show}"), Permissions(Read))
      )
      val results = instanceQueries.list(Pagination(0, pageSize), domainId, None, None, acls).futureValue
      results.total shouldEqual 0
    }

    "return correct count if the user has permissions for organization and domain within that organization" in {
      val domainId: DomainId = instanceIds.head.schemaId.domainId
      val acls = FullAccessControlList(
        (Identity.Anonymous(), Path(s"/kg/${domainId.show}"), Permissions(Read)),
        (Identity.Anonymous(), Path(s"/kg/${domainId.orgId.show}"), Permissions(Read))
      )

      val expectedTotal = instanceIds.count(_.schemaId.domainId.orgId == domainId.orgId)
      val results       = instanceQueries.list(Pagination(0, pageSize), domainId.orgId, None, None, acls).futureValue
      results.total shouldEqual expectedTotal
    }

    "return 0 if the user has no permissions for domain in which the schema is nested" in {
      val schemaId: SchemaId  = instanceIds.head.schemaId
      val schemaId2: SchemaId = instanceIds.last.schemaId
      val acls = FullAccessControlList(
        (Identity.Anonymous(), Path(s"/kg/${schemaId2.domainId.show}"), Permissions(Read))
      )
      val results = instanceQueries.list(Pagination(0, pageSize), schemaId, None, None, acls).futureValue
      results.total shouldEqual 0
    }

  }

}
