package ch.epfl.bluebrain.nexus.kg.async

import java.time.Clock

import akka.testkit._
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.InProjectResolver
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Inspectors, Matchers}

import scala.concurrent.duration._

class ResolverCacheSpec
    extends ActorSystemFixture("ResolverCacheSpec", true)
    with Randomness
    with Matchers
    with Inspectors
    with ScalaFutures
    with TestHelper {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3.seconds.dilated, 5.milliseconds)

  private implicit val clock = Clock.systemUTC

  private implicit val appConfig = Settings(system).appConfig

  val ref1 = ProjectRef(genUUID)
  val ref2 = ProjectRef(genUUID)

  val resolver: InProjectResolver = InProjectResolver(ref1, genIri, 1L, false, 10)

  val resolverProj1 = List.fill(5)(resolver.copy(id = genIri)).toSet
  val resolverProj2 = List.fill(5)(resolver.copy(id = genIri, ref = ref2)).toSet

  private val cache = ResolverCache[Task]

  "ResolverCache" should {

    "index resolvers" in {
      forAll(resolverProj1 ++ resolverProj2) { resolver =>
        cache.put(resolver).runToFuture.futureValue
        cache.get(resolver.ref, resolver.id).runToFuture.futureValue shouldEqual Some(resolver)
      }
    }

    "list resolvers" in {
      cache.get(ref1).runToFuture.futureValue should contain theSameElementsAs resolverProj1
      cache.get(ref2).runToFuture.futureValue should contain theSameElementsAs resolverProj2
    }

    "deprecate resolver" in {
      val resolver = resolverProj1.head
      cache.put(resolver.copy(deprecated = true, rev = 2L)).runToFuture.futureValue
      cache.get(resolver.ref, resolver.id).runToFuture.futureValue shouldEqual None
      cache.get(ref1).runToFuture.futureValue should contain theSameElementsAs resolverProj1.filterNot(_ == resolver)
    }
  }
}
