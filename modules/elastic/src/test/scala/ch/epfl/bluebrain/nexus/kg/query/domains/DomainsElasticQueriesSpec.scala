package ch.epfl.bluebrain.nexus.kg.query.domains

import java.util.regex.Pattern.quote

import cats.syntax.show._
import akka.http.scaladsl.client.RequestBuilding.Post
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.query.QueryFixture
import io.circe.{Decoder, Json}

class DomainsElasticQueriesSpec extends QueryFixture[DomainId] {

  override implicit lazy val idDecoder: Decoder[DomainId] = elasticIdDecoder

  val domainsQueries = DomainsElasticQueries(elasticClient, settings)
  val doms: Map[(DomainId, Boolean), Json] = (for {
    orgCount <- 1 to 2
    orgName = genId()
    domCount <- 1 to objectCount
    domName    = genId()
    domId      = DomainId(OrgId(orgName), domName)
    deprecated = math.random < 0.5
    domJson = jsonContentOf("/domains/domain_source.json",
                            Map(quote("{{orgName}}")    -> orgName,
                                quote("{{domainName}}") -> domName,
                                quote("{{deprecated}}") -> deprecated.toString))
  } yield (domId, deprecated) -> domJson).toMap

  implicit val ord: Ordering[DomainId] = (x: DomainId, y: DomainId) => x.show.compare(y.show)

  override def beforeAll(): Unit = {
    super.beforeAll()
    elasticClient.createIndex("domains", mapping).futureValue
    doms.foreach {
      case ((domId, _), domJson) =>
        elasticClient.create("domains", "doc", s"domainid_${domId.show}", domJson).futureValue
    }
    val _ = untypedHttpClient(Post(refreshUri)).futureValue
  }

  "DomainsElasticQueries" should {

    "list all domains with pagination" in {
      val domIds = doms.keys.map(_._1).toList.sorted
      var i      = 0L
      domIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = domainsQueries.list(Pagination(i, pageSize), None, None).futureValue
        results.total shouldEqual domIds.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list non deprecated domains with pagination" in {
      val domIds = doms.keys.filterNot(_._2).map(_._1).toList.sorted
      var i      = 0L
      domIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = domainsQueries.list(Pagination(i, pageSize), Some(false), None).futureValue
        results.total shouldEqual domIds.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list deprecated domains with pagination" in {
      val domIds = doms.keys.filter(_._2).map(_._1).toList.sorted
      var i      = 0L
      domIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = domainsQueries.list(Pagination(i, pageSize), Some(true), None).futureValue
        results.total shouldEqual domIds.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list all domains for an organization" in {
      val domIds                                  = doms.keys.map(_._1).toList.sorted
      val domsByOrgId: Map[OrgId, List[DomainId]] = domIds.groupBy(_.orgId)
      domsByOrgId.foreach {
        case (org, ds) =>
          var i = 0L
          ds.sliding(pageSize, pageSize).foreach { ids =>
            val results = domainsQueries.list(Pagination(i, pageSize), org, None, None).futureValue
            results.total shouldEqual ds.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list deprecated domains for an organization" in {
      val domIds                                  = doms.keys.filter(_._2).map(_._1).toList.sorted
      val domsByOrgId: Map[OrgId, List[DomainId]] = domIds.groupBy(_.orgId)
      domsByOrgId.foreach {
        case (org, ds) =>
          var i = 0L
          ds.sliding(pageSize, pageSize).foreach { ids =>
            val results = domainsQueries.list(Pagination(i, pageSize), org, Some(true), None).futureValue
            results.total shouldEqual ds.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list non deprecated domains for an organization" in {
      val domIds                                  = doms.keys.filterNot(_._2).map(_._1).toList.sorted
      val domsByOrgId: Map[OrgId, List[DomainId]] = domIds.groupBy(_.orgId)
      domsByOrgId.foreach {
        case (org, ds) =>
          var i = 0L
          ds.sliding(pageSize, pageSize).foreach { ids =>
            val results = domainsQueries.list(Pagination(i, pageSize), org, Some(false), None).futureValue
            results.total shouldEqual ds.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }
  }
}
