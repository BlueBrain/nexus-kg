package ch.epfl.bluebrain.nexus.kg.async

import java.time.{Clock, Instant}
import java.util.UUID

import akka.testkit._
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, Randomness}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.StorageCacheSpec.StorageA
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.kg.resources.file.Storage
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Inspectors, Matchers, TryValues}

import scala.concurrent.duration._

//noinspection NameBooleanParameters
class StorageCacheSpec
    extends ActorSystemFixture("StorageCacheSpec", true)
    with Randomness
    with Matchers
    with Inspectors
    with ScalaFutures
    with TryValues
    with TestHelper {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3.seconds.dilated, 5.milliseconds)

  private implicit val clock: Clock         = Clock.systemUTC
  private implicit val appConfig: AppConfig = Settings(system).appConfig

  val ref1 = ProjectRef(genUUID)
  val ref2 = ProjectRef(genUUID)

  val initialInstant = clock.instant()
  val lastIdA        = url"http://example.com/lastA".value

  val storage = StorageA(ref1, genIri, genUUID, 1L, false, true, initialInstant.minusSeconds(1L + genInt().toLong))

  val lastStorageProj1 = storage.copy(id = lastIdA, uuid = genUUID, instant = initialInstant)
  val lastStorageProj2 = storage.copy(ref = ref2, id = lastIdA, uuid = genUUID, instant = initialInstant)

  val storagesProj1: List[StorageA] =
    lastStorageProj1 :: List.fill(5)(
      storage.copy(id = genIri, uuid = genUUID, instant = initialInstant.minusSeconds(1L + genInt().toLong)))
  val storagesProj2: List[StorageA] =
    lastStorageProj2 :: List.fill(5)(
      storage
        .copy(ref = ref2, id = genIri, uuid = genUUID, instant = initialInstant.minusSeconds(1L + genInt().toLong)))

  private val cache = StorageCache[Task]

  "StorageCache" should {

    "index storages" in {
      forAll(storagesProj1 ++ storagesProj2) { storage =>
        cache.put(storage).runToFuture.futureValue
        cache.get(storage.ref, storage.id).runToFuture.futureValue shouldEqual Some(storage)
      }
    }

    "get latest default storage" in {
      cache.getDefault(ref1).runToFuture.futureValue shouldEqual Some(lastStorageProj1)
      cache.getDefault(ref2).runToFuture.futureValue shouldEqual Some(lastStorageProj2)
      cache.getDefault(ProjectRef(genUUID)).runToFuture.futureValue shouldEqual None
    }

    "list storages" in {
      cache.get(ref1).runToFuture.futureValue should contain theSameElementsAs storagesProj1
      cache.get(ref2).runToFuture.futureValue should contain theSameElementsAs storagesProj2
    }

    "deprecate storage" in {
      val storage = storagesProj1.head
      cache.put(storage.copy(deprecated = true, rev = 2L)).runToFuture.futureValue
      cache.get(storage.ref, storage.id).runToFuture.futureValue shouldEqual None
      cache.get(ref1).runToFuture.futureValue should contain theSameElementsAs storagesProj1.filterNot(_ == storage)
    }
  }
}

object StorageCacheSpec {
  final case class StorageA(ref: ProjectRef,
                            id: AbsoluteIri,
                            uuid: UUID,
                            rev: Long,
                            deprecated: Boolean,
                            default: Boolean,
                            instant: Instant)
      extends Storage
}
