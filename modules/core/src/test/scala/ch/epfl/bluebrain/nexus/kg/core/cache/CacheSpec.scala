package ch.epfl.bluebrain.nexus.kg.core.cache

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.kg.core.cache.CacheAkka.CacheSettings
import ch.epfl.bluebrain.nexus.kg.core.cache.CacheError.EmptyKey
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class CacheSpec
    extends TestKit(ActorSystem("CacheAkkaSpec"))
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {
  override implicit val patienceConfig = PatienceConfig(6 seconds, 100 millis)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Cluster(system).join(Cluster(system).selfAddress)
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val cache: Cache[Future, Int] = CacheAkka[Int]("some", CacheSettings())

  "A Cache" should {

    "add elements" in {
      cache.put("one", 1).futureValue shouldEqual (())
      cache.put("two", 2).futureValue shouldEqual (())
      cache.put("three", 3).futureValue shouldEqual (())
      cache.put("one", 4).futureValue shouldEqual (())
    }
    "fetch the added elements" in {
      cache.get("one").futureValue shouldEqual Some(4)
      cache.get("two").futureValue shouldEqual Some(2)
      cache.get("three").futureValue shouldEqual Some(3)
      cache.get("five").futureValue shouldEqual None
    }

    "remove elements" in {
      cache.remove("one").futureValue shouldEqual (())
      cache.get("one").futureValue shouldEqual None
      cache.remove("a").futureValue shouldEqual (())
      whenReady(cache.remove("").failed)(e => e shouldEqual EmptyKey)
    }
  }
}
