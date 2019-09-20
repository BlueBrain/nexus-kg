package ch.epfl.bluebrain.nexus.kg.archives

import java.time.{Clock, Instant, ZoneId}

import cats.effect.IO
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.commons.test.io.IOOptionValues
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.archives.Archive.{File, Resource, ResourceDescription}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ArchiveCacheConfig
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.resources.Id
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class ArchiveCacheSpec
    extends ActorSystemFixture("ArchiveCacheSpec", true)
    with TestHelper
    with WordSpecLike
    with Matchers
    with IOOptionValues
    with Eventually {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(10 second, 50 milliseconds)

  private val appConfig = Settings(system).appConfig
  private implicit val config =
    appConfig.copy(archiveCache = ArchiveCacheConfig(3 second, 500 millis, 100))

  private val cache: ArchiveCache[IO] = ArchiveCache[IO]()
  private implicit val clock          = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault())
  private val instant                 = clock.instant()

  def randomProject() = {
    val instant = Instant.EPOCH
    // format: off
    Project(genIri, genString(), genString(), None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, false, instant, genIri, instant, genIri)
    // format: on
  }

  "An archive cache" should {

    "write and read an Archive" in {
      val resId     = Id(randomProject().ref, genIri)
      val resource1 = Resource(genIri, randomProject(), None, None, originalSource = true, None)
      val file1     = File(genIri, randomProject(), None, None, None)
      val archive   = Archive(resId, instant, Anonymous, Set(resource1, file1))
      val _         = cache.put(archive).value.some
      cache.get(archive.resId).value.some shouldEqual archive
    }

    "read a non existing resource" in {
      val resId = Id(randomProject().ref, genIri)
      cache.get(resId).value.ioValue shouldEqual None
    }

    "read after timeout" in {
      val resId   = Id(randomProject().ref, genIri)
      val set     = Set[ResourceDescription](Resource(genIri, randomProject(), None, None, originalSource = true, None))
      val archive = Archive(resId, instant, Anonymous, set)
      val _       = cache.put(archive).value.some
      val time    = System.currentTimeMillis()
      cache.get(resId).value.some shouldEqual archive
      eventually {
        cache.get(resId).value.ioValue shouldEqual None
      }
      val diff = System.currentTimeMillis() - time
      diff should be > config.archiveCache.invalidateAfter.toMillis
      diff should be < config.archiveCache.invalidateAfter.toMillis + 300
    }
  }
}
