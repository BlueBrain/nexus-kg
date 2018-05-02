package ch.epfl.bluebrain.nexus.kg.core.resources

import java.time.Clock

import ch.epfl.bluebrain.nexus.commons.types.Meta
import ch.epfl.bluebrain.nexus.commons.types.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.kg.core.resources.Command._
import ch.epfl.bluebrain.nexus.kg.core.resources.Payload._
import ch.epfl.bluebrain.nexus.kg.core.resources.ResourceRejection._
import ch.epfl.bluebrain.nexus.kg.core.resources.State._
import ch.epfl.bluebrain.nexus.kg.core.resources.attachment.Attachment._
import eu.timepit.refined.auto._
import io.circe.Json
import org.scalatest.{EitherValues, Inspectors, Matchers, WordSpecLike}

class StateSpec extends WordSpecLike with Matchers with Inspectors with EitherValues {

  "A State" should {

    val key =
      RepresentationId("projectName",
                       "https://bbp.epfl.ch/nexus/data/resourceName",
                       "https://bbp.epfl.ch/nexus/schemas/schemaName")
    val meta        = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())
    val meta2       = Meta(UserRef("realm", "sub:5678"), Clock.systemUTC.instant())
    val value       = Json.obj("key" -> Json.obj("value" -> Json.fromString("seodhkxtudwlpnwb")))
    val payload     = JsonPayload(value)
    val attachment  = BinaryAttributes("uri", "filename", "mediaType", Size("MB", 123L), Digest("SHA256", "ABCDEF"))
    val attachment2 = BinaryAttributes("uri2", "filename2", "mediaType", Size("MB", 456L), Digest("SHA256", "FEDCBA"))

    "evaluate a command and generate the next state" in {

      val mapping: List[(State, Command, State)] = List(
        (Initial, Create(key, 1L, meta, payload), Current(key, 1L, meta, payload, Set.empty, false, Map.empty)),
        (Current(key, 1L, meta, payload, Set.empty, true, Map.empty),
         Undeprecate(key, 1L, meta2),
         Current(key, 2L, meta2, payload, Set.empty, false, Map.empty)),
        (Current(key, 3L, meta, payload, Set.empty, false, Map.empty),
         Tag(key, 1L, meta2, "tag"),
         Current(key, 3L, meta2, payload, Set.empty, false, Map("tag" -> 1L))),
        (Current(key, 3L, meta, payload, Set.empty, false, Map("tag"  -> 2L)),
         Tag(key, 1L, meta2, "tag2"),
         Current(key, 3L, meta2, payload, Set.empty, false, Map("tag2" -> 1L, "tag" -> 2L))),
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Replace(key, 1L, meta2, TurtlePayload("")),
         Current(key, 2L, meta2, TurtlePayload(""), Set.empty, false, Map.empty)),
        (Current(key, 3L, meta, payload, Set.empty, false, Map("tag" -> 2L)),
         Deprecate(key, 3L, meta2),
         Current(key, 4L, meta2, payload, Set.empty, true, Map("tag"       -> 2L))),
        (Current(key, 3L, meta, payload, Set(attachment), false, Map("tag" -> 2L)),
         Attach(key, 3L, meta2, attachment2),
         Current(key, 4L, meta2, payload, Set(attachment, attachment2), false, Map("tag" -> 2L))),
        (Current(key, 3L, meta, payload, Set(attachment, attachment2), false, Map("tag"  -> 2L)),
         Unattach(key, 3L, meta2, "filename"),
         Current(key, 4L, meta2, payload, Set(attachment2), false, Map("tag" -> 2L)))
      )

      forAll(mapping) {
        case (state, cmd, next) =>
          State.eval(state, cmd).map(ev => State.next(state, ev)).right.value shouldEqual next
      }
    }

    "reject invalid commands" in {

      val mapping: List[(State, Command, ResourceRejection)] = List(
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Create(key, 1L, meta, payload),
         ResourceAlreadyExists),
        (Initial, Replace(key, 1L, meta2, TurtlePayload("")), ResourceDoesNotExists),
        (Initial, Undeprecate(key, 1L, meta2), ResourceDoesNotExists),
        (Initial, Deprecate(key, 1L, meta2), ResourceDoesNotExists),
        (Initial, Tag(key, 1L, meta2, "name"), ResourceDoesNotExists),
        (Initial, Attach(key, 1L, meta2, attachment), ResourceDoesNotExists),
        (Initial, Unattach(key, 1L, meta2, "at"), ResourceDoesNotExists),
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Replace(key, 2L, meta2, payload),
         IncorrectRevisionProvided),
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Deprecate(key, 0L, meta2),
         IncorrectRevisionProvided),
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Undeprecate(key, 10L, meta2),
         IncorrectRevisionProvided),
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Tag(key, 10L, meta2, "name"),
         IncorrectRevisionProvided),
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Attach(key, 10L, meta2, attachment),
         IncorrectRevisionProvided),
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Unattach(key, 10L, meta2, "at"),
         IncorrectRevisionProvided),
        (Current(key, 1L, meta, payload, Set.empty, true, Map.empty),
         Replace(key, 1L, meta2, payload),
         ResourceIsDeprecated),
        (Current(key, 1L, meta, payload, Set.empty, true, Map.empty),
         Attach(key, 1L, meta2, attachment),
         ResourceIsDeprecated),
        (Current(key, 1L, meta, payload, Set.empty, true, Map.empty),
         Unattach(key, 1L, meta2, "name"),
         ResourceIsDeprecated),
        (Current(key, 3L, meta, payload, Set(attachment), false, Map("tag" -> 2L)),
         Unattach(key, 3L, meta2, "nonexistant"),
         AttachmentDoesNotExists),
        (Current(key, 1L, meta, payload, Set.empty, true, Map.empty), Deprecate(key, 1L, meta2), ResourceIsDeprecated),
        (Current(key, 1L, meta, payload, Set.empty, false, Map.empty),
         Undeprecate(key, 1L, meta2),
         ResourceIsNotDeprecated)
      )

      forAll(mapping) {
        case (state, cmd, rejection) =>
          State.eval(state, cmd).left.value shouldEqual rejection
      }
    }
  }

}
