package ch.epfl.bluebrain.nexus.kg.persistence

import java.time.{Clock, Instant, ZoneId}

import akka.persistence.journal.Tagged
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.persistence.TaggingAdapterSpec.Other
import ch.epfl.bluebrain.nexus.kg.resources.Event._
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef, Ref}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class TaggingAdapterSpec extends WordSpecLike with Matchers with Inspectors with Randomness {

  "A TaggingAdapter" should {
    val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

    def genJson(): Json = Json.obj("key" -> Json.fromString(genString()))

    val adapter = new TaggingAdapter()
    val id      = Id(ProjectRef("uuid"), nxv.projects)

    val mapping = Map(
      Set(s"type=${nxv.Schema.value.show}", s"type=${nxv.Resource.value.show}", "project=uuid") ->
        Created(id, 1L, Ref(shaclSchemaUri), Set(nxv.Schema, nxv.Resource), genJson(), clock.instant(), Anonymous),
      Set(s"type=${nxv.Resolver.value.show}", s"type=${nxv.Resource.value.show}", "project=uuid") ->
        Updated(id, 1L, Set(nxv.Resource, nxv.Resolver), genJson(), clock.instant(), Anonymous),
      Set(s"type=${nxv.Resource.value.show}", "project=uuid") ->
        Deprecated(id, 1L, Set(nxv.Resource), clock.instant(), Anonymous),
      Set("project=uuid") ->
        TagAdded(id, 2L, 1L, "tag", clock.instant(), Anonymous)
    )

    "set the appropriate tags" in {
      forAll(mapping.toList) {
        case (tags, ev) => adapter.toJournal(ev) shouldEqual Tagged(ev, tags)
      }
    }

    "return an empty manifest" in {
      adapter.manifest(Other(genString())) shouldEqual ""
    }
  }
}

object TaggingAdapterSpec {
  private[persistence] final case class Other(value: String)

}
