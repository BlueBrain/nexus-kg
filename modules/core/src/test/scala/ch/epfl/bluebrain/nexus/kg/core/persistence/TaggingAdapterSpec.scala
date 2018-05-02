package ch.epfl.bluebrain.nexus.kg.core.persistence

import java.time.Clock

import akka.persistence.journal.Tagged
import ch.epfl.bluebrain.nexus.commons.types.Meta
import ch.epfl.bluebrain.nexus.commons.types.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.core.resources.Event._
import ch.epfl.bluebrain.nexus.kg.core.resources.Payload.JsonPayload
import ch.epfl.bluebrain.nexus.kg.core.resources.{Event, RepresentationId}
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class TaggingAdapterSpec extends WordSpecLike with Matchers with Inspectors with Randomness {

  def genJson(): Json = Json.obj("key" -> Json.fromString(genString()))

  private case class OtherEvent(some: String)

  "A TaggingAdapter" should {

    val adapter = new TaggingAdapter()

    val key =
      RepresentationId("projectName",
                       "https://bbp.epfl.ch/nexus/data/resourceName",
                       "https://bbp.epfl.ch/nexus/schemas/schemaName")
    val meta  = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
    val value = Json.obj("key" -> Json.obj("value" -> Json.fromString("seodhkxtudwlpnwb")))

    val mapping = Map(
      Created(key, 1L, meta, JsonPayload(value), Set("a", "b", "c")) -> Set("a", "b", "c"),
      Replaced(key, 1L, meta, JsonPayload(value), Set("a", "d"))     -> Set("a", "d"),
      Deprecated(key, 1L, meta, Set("c"))                            -> Set("c"),
      Undeprecated(key, 1L, meta, Set("y"))                          -> Set("y"),
      Event.Tagged(key, 1L, meta, "namedTag", Set("a", "z"))         -> Set("a", "z")
    )

    "set the appropriate tags" in {
      forAll(mapping.toList) {
        case (ev, tags) => adapter.toJournal(ev) shouldEqual Tagged(ev, tags)
      }
    }

    "return an empty manifest" in {
      adapter.manifest(OtherEvent(genString())) shouldEqual ""
    }

  }
}
