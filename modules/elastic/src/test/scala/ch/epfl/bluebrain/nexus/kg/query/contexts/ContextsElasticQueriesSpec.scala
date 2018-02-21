package ch.epfl.bluebrain.nexus.kg.query.contexts

import java.util.regex.Pattern.quote

import akka.http.scaladsl.client.RequestBuilding.Post
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.kg.ElasticIdDecoder.elasticIdDecoder
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.query.QueryFixture
import io.circe.{Decoder, Json}

class ContextsElasticQueriesSpec extends QueryFixture[ContextId] {
  override implicit lazy val idDecoder: Decoder[ContextId] = elasticIdDecoder

  val contextQueries = ContextsElasticQueries(elasticClient, settings)
  val contexts: Map[ContextId, Json] = (for {
    orgCount <- 1 to 2
    orgName = genId()
    domCount <- 1 to 2
    domName = genId()
    contextCount <- 1 to objectCount
    contextName = genId()
    contextVersion <- List(Version(0, 0, 1), Version(0, 0, 2))
    domId     = DomainId(OrgId(orgName), domName)
    contextId = ContextId(domId, contextName, contextVersion)
    contextJson = jsonContentOf(
      "/contexts/context_source.json",
      Map(
        quote("{{orgName}}")     -> orgName,
        quote("{{domainName}}")  -> domName,
        quote("{{contextName}}") -> contextName,
        quote("{{version}}")     -> contextVersion.show,
      )
    )
  } yield contextId -> contextJson).toMap
  implicit val ord: Ordering[ContextId] = (x: ContextId, y: ContextId) => x.show.compare(y.show)
  val contextIds: List[ContextId]       = contexts.keys.toList.sorted

  override def beforeAll(): Unit = {
    super.beforeAll()
    elasticClient.createIndex(ElasticIds.contextsIndex(elasticPrefix), mapping).futureValue
    contexts.foreach {
      case (contextId, contextJson) =>
        elasticClient.create(contextId.toIndex(elasticPrefix), "doc", contextId.elasticId, contextJson).futureValue
    }
    val _ = untypedHttpClient(Post(refreshUri)).futureValue
  }

  "ContextsElasticQueries" should {
    "list all contexts with pagination" in {
      var i = 0L
      contextIds.sliding(pageSize, pageSize).foreach { ids =>
        val results = contextQueries.list(Pagination(i, pageSize), None, None, defaultAcls).futureValue
        results.total shouldEqual contexts.size
        results.results.map(_.source) shouldEqual ids
        i = i + ids.size
      }
    }

    "list all contexts for organization with pagination" in {
      val contextsByOrgId: Map[OrgId, List[ContextId]] = contextIds.groupBy(_.domainId.orgId)
      contextsByOrgId.foreach {
        case (org, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = contextQueries.list(Pagination(i, pageSize), org, None, None, defaultAcls).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all contexts for domain with pagination" in {
      val contextsByDomainId: Map[DomainId, List[ContextId]] = contextIds.groupBy(_.domainId)
      contextsByDomainId.foreach {
        case (dom, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = contextQueries.list(Pagination(i, pageSize), dom, None, None, defaultAcls).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }

    "list all contexts for context group with pagination" in {
      val contextsByContextName: Map[ContextName, List[ContextId]] = contextIds.groupBy(_.contextName)
      contextsByContextName.foreach {
        case (name, cs) =>
          var i = 0L
          cs.sliding(pageSize, pageSize).foreach { ids =>
            val results = contextQueries.list(Pagination(i, pageSize), name, None, None, defaultAcls).futureValue
            results.total shouldEqual cs.size
            results.results.map(_.source) shouldEqual ids
            i = i + ids.size

          }
      }
    }
  }
}
