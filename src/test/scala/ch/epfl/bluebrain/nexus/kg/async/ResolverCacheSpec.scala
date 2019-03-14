package ch.epfl.bluebrain.nexus.kg.async

import java.time.Clock

import akka.actor.ExtendedActorSystem
import akka.serialization.Serialization
import akka.testkit._
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectLabel, ProjectRef}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Inspectors, Matchers, TryValues}

import scala.concurrent.duration._

//noinspection NameBooleanParameters
class ResolverCacheSpec
    extends ActorSystemFixture("ResolverCacheSpec", true)
    with Randomness
    with Matchers
    with Inspectors
    with ScalaFutures
    with TryValues
    with TestHelper
    with Eventually {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3.seconds.dilated, 5.milliseconds)

  private implicit val clock: Clock         = Clock.systemUTC
  private implicit val appConfig: AppConfig = Settings(system).appConfig
  private val time                          = clock.instant()

  val ref1 = ProjectRef(genUUID)
  val ref2 = ProjectRef(genUUID)

  val label1 = ProjectLabel(genString(), genString())
  val label2 = ProjectLabel(genString(), genString())

  val resolver: InProjectResolver = InProjectResolver(ref1, genIri, 1L, false, 10)
  val crossRefs: CrossProjectResolver[ProjectRef] =
    CrossProjectResolver(Set(genIri), Set(ref1, ref2), List(Anonymous), ref1, genIri, 0L, false, 1)
  val crossLabels: CrossProjectResolver[ProjectLabel] =
    CrossProjectResolver(Set(genIri), Set(label1, label2), List(Anonymous), ref1, genIri, 0L, false, 1)

  val resolverProj1: Set[InProjectResolver] = List.fill(5)(resolver.copy(id = genIri)).toSet
  val resolverProj2: Set[InProjectResolver] = List.fill(5)(resolver.copy(id = genIri, ref = ref2)).toSet

  private val cache = ResolverCache[Task]

  "ResolverCache" should {

    "index resolvers" in {
      val list = (resolverProj1 ++ resolverProj2).toList
      forAll(list.zipWithIndex) {
        case (resolver, index) =>
          implicit val instant = time.plusSeconds(index.toLong)
          cache.put(resolver).runToFuture.futureValue
          eventually {
            cache.get(resolver.ref, resolver.id).runToFuture.futureValue shouldEqual Some(resolver)
          }
      }
    }

    "list resolvers" in {
      cache.get(ref1).runToFuture.futureValue should contain theSameElementsAs resolverProj1
      cache.get(ref2).runToFuture.futureValue should contain theSameElementsAs resolverProj2
    }

    "deprecate resolver" in {
      val resolver         = resolverProj1.head
      implicit val instant = time.plusSeconds(300L)
      cache.put(resolver.copy(deprecated = true, rev = 2L)).runToFuture.futureValue
      cache.get(resolver.ref, resolver.id).runToFuture.futureValue shouldEqual None
      cache.get(ref1).runToFuture.futureValue should contain theSameElementsAs resolverProj1.filterNot(_ == resolver)
    }

    "serialize cross project resolver" when {
      val serialization = new Serialization(system.asInstanceOf[ExtendedActorSystem])
      "parameterized with ProjectRef" in {
        val bytes = serialization.serialize(crossRefs).success.value
        val out   = serialization.deserialize(bytes, classOf[CrossProjectResolver[ProjectRef]]).success.value
        out shouldEqual crossRefs
      }
      "parameterized with ProjectLabel" in {
        val bytes = serialization.serialize(crossLabels).success.value
        val out   = serialization.deserialize(bytes, classOf[CrossProjectResolver[ProjectLabel]]).success.value
        out shouldEqual crossLabels
      }
    }
  }
}
