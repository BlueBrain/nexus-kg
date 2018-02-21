package ch.epfl.bluebrain.nexus.kg.query.organizations

import java.util.regex.Pattern.quote

import cats.instances.future._
import cats.syntax.show._
import akka.http.scaladsl.client.RequestBuilding._
import ch.epfl.bluebrain.nexus.commons.iam.acls.{FullAccessControlList, Path, Permissions}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.Read
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.query.QueryFixture
import io.circe.{Decoder, Json}

class OrganizationsElasticQueriesSpec extends QueryFixture[OrgId] {

  override implicit lazy val idDecoder: Decoder[OrgId] = elasticIdDecoder

  val organizationQueries = OrganizationsElasticQueries(elasticClient, settings)
  val orgs: Map[(OrgId, Boolean), Json] = (for {
    _ <- 1 to objectCount
    orgName    = genId()
    orgId      = OrgId(orgName)
    deprecated = math.random < 0.5
    orgJson = jsonContentOf("/organizations/organization_source.json",
                            Map(quote("{{orgName}}") -> orgName, quote("{{deprecated}}") -> deprecated.toString))
  } yield (orgId, deprecated) -> orgJson).toMap

  implicit val ord: Ordering[OrgId] = (x: OrgId, y: OrgId) => x.show.compare(y.show)

  override def beforeAll(): Unit = {
    super.beforeAll()
    elasticClient.createIndex(ElasticIds.organizationsIndex(elasticPrefix), mapping).futureValue
    orgs.foreach {
      case ((orgId, _), orgJson) =>
        elasticClient.create(orgId.toIndex(elasticPrefix), "doc", orgId.elasticId, orgJson).futureValue
    }
    val _ = untypedHttpClient(Post(refreshUri)).futureValue
  }

  "OrganizationElasticQueries" should {

    "list all organizations with pagination" in {

      val orgIds: List[OrgId] = orgs.keys.map(_._1).toList.sorted

      var i = 0L
      orgIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = organizationQueries.list(Pagination(i, pageSize), None, None, defaultAcls).futureValue
        results.total shouldEqual orgs.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list only organizations the user has access to" in {

      val orgIds: List[OrgId] = orgs.keys.map(_._1).toList.sorted
      val orgId1              = orgIds.head
      val orgId2              = orgIds.last

      val acls = FullAccessControlList(
        (Identity.Anonymous(), Path(s"/kg/${orgId2.show}"), Permissions(Read)),
        (Identity.Anonymous(), Path(s"/kg/${orgId1.show}"), Permissions(Read))
      )

      val results = organizationQueries.list(Pagination(0, pageSize), None, None, acls).futureValue
      results.total shouldEqual 2
      results.results.map(_.source) shouldEqual Seq(orgId1, orgId2)
    }

    "list deprecated organizations with pagination" in {

      val orgIds: List[OrgId] = orgs.keys.filter(_._2).map(_._1).toList.sorted

      var i = 0L
      orgIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = organizationQueries.list(Pagination(i, pageSize), Some(true), None, defaultAcls).futureValue
        results.total shouldEqual orgIds.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list non deprecated organizations with pagination" in {

      val orgIds: List[OrgId] = orgs.keys.filterNot(_._2).map(_._1).toList.sorted

      var i = 0L
      orgIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = organizationQueries.list(Pagination(i, pageSize), Some(false), None, defaultAcls).futureValue
        results.total shouldEqual orgIds.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

  }
}
