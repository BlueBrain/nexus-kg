package ch.epfl.bluebrain.nexus.kg.core.queries

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.http.scaladsl.model.Uri
import akka.testkit.TestKit
import cats.instances.future._
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.cache.ShardedCache.CacheSettings
import ch.epfl.bluebrain.nexus.kg.core.cache.{Cache, ShardedCache}
import ch.epfl.bluebrain.nexus.kg.core.queries.Query.QueryPayload
import ch.epfl.bluebrain.nexus.kg.core.queries.QueryRejection._
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr.ComparisonExpr
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Filter
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op.Eq
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.LiteralTerm
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.Future
import scala.concurrent.duration._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
class QueriesSpec
    extends TestKit(ActorSystem("QueriesSpec"))
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with CancelAfterFailure
    with Inspectors
    with Assertions {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(6 seconds, 100 millis)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).join(Cluster(system).selfAddress)
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val nexusBaseVoc: Uri = s"https://bbp-nexus.epfl.ch/vocabs/nexus/core/terms/v0.1.0/"
  import system.dispatcher

  "A Queries instance" should {
    val cache: Cache[Future, Query] = ShardedCache[Query]("some", CacheSettings())
    val queries                     = Queries(cache)
    val defaultQuery                = QueryPayload()
    val queryFilter =
      QueryPayload(filter = Filter(ComparisonExpr(Eq, UriPath(s"${nexusBaseVoc}deprecated"), LiteralTerm("false"))))

    var defaultQueryId = QueryId("")
    var queryFilterId  = QueryId("")
    var list           = List.empty[Query]

    "create queries" in {
      defaultQueryId = queries.create(/, defaultQuery).futureValue
      queryFilterId = queries.create("a" / "b", queryFilter).futureValue
      list = List(Query(defaultQueryId, /, defaultQuery), Query(queryFilterId, "a" / "b", queryFilter))
    }

    "get queries" in {
      forAll(list) {
        case q @ Query(id, _, _) => queries.fetch(id).futureValue shouldEqual Some(q)
      }
    }

    "return None when fetching a query that does not exist" in {
      queries.fetch(QueryId("not_exist")).futureValue shouldEqual None
    }
    val defaultQueryUpdated = QueryPayload(q = Some("text"), deprecated = Some(false), resource = QueryResource.Domains)

    "update a query" in {
      queries.update(defaultQueryId, defaultQueryUpdated).futureValue shouldEqual defaultQueryId

      queries.fetch(defaultQueryId).futureValue shouldEqual Some(Query(defaultQueryId, /, defaultQueryUpdated))
    }

    "prevent updating a query that does not exist" in {
      whenReady(queries.update(QueryId("some"), defaultQueryUpdated).failed) { e =>
        e shouldEqual CommandRejected(QueryDoesNotExist)
      }

    }
  }
}
