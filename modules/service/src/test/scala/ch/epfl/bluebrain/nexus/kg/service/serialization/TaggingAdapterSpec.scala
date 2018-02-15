package ch.epfl.bluebrain.nexus.kg.service.serialization

import java.time.Clock

import akka.persistence.journal.Tagged
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.service.contexts.ContextEvent.ContextDeprecated
import ch.epfl.bluebrain.nexus.kg.service.contexts.ContextId
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectEvent.ProjectDeprecated
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectId
import ch.epfl.bluebrain.nexus.kg.service.schemas.SchemaEvent.SchemaDeprecated
import ch.epfl.bluebrain.nexus.kg.service.schemas.SchemaId
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class TaggingAdapterSpec extends WordSpecLike with Matchers with Inspectors with Randomness {

  "A TaggingAdapter" should {

    val adapter = new TaggingAdapter()

    val projectId = ProjectId(genString()).get
    val schemaId  = SchemaId(projectId, genString()).get
    val contextId = ContextId(projectId, genString()).get
    val rev       = 1L
    val meta      = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())

    val mapping = Map(
      ProjectDeprecated(projectId, rev, meta) -> "project",
      SchemaDeprecated(schemaId, rev, meta)   -> "schema",
      ContextDeprecated(contextId, rev, meta) -> "context"
    )

    "set the appropriate tags" in {
      forAll(mapping.toList) {
        case (ev, tag) => adapter.toJournal(ev) shouldEqual Tagged(ev, Set(tag))
      }
    }

    "ignore other types" in {
      adapter.toJournal(schemaId) shouldEqual schemaId
    }

    "return an empty manifest" in {
      adapter.manifest(ProjectDeprecated(projectId, rev, meta)) shouldEqual ""
    }
  }

}
