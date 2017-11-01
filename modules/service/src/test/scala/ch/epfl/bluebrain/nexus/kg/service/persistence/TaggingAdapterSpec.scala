package ch.epfl.bluebrain.nexus.kg.service.persistence

import java.time.Clock

import akka.persistence.journal.Tagged
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextEvent, ContextId}
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainEvent, DomainId}
import ch.epfl.bluebrain.nexus.kg.core.instances.{InstanceEvent, InstanceId}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgEvent, OrgId}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaEvent, SchemaId}
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class TaggingAdapterSpec extends WordSpecLike with Matchers with Inspectors {

  "A TaggingAdapter" should {

    val adapter = new TaggingAdapter()

    val id        = "id"
    val orgId     = OrgId(id)
    val domId     = DomainId(orgId, id)
    val schemaId  = SchemaId(domId, "name", Version(1, 1, 1))
    val contextId = ContextId(domId, "name", Version(1, 1, 1))
    val instId    = InstanceId(schemaId, id)
    val rev       = 1L
    val meta      = Meta(UserRef("realm", "sub:1234"), Clock.systemUTC.instant())

    val mapping = Map(
      OrgEvent.OrgDeprecated(orgId, rev, meta)             -> "organization",
      DomainEvent.DomainDeprecated(domId, rev, meta)       -> "domain",
      SchemaEvent.SchemaDeprecated(schemaId, rev, meta)    -> "schema",
      ContextEvent.ContextDeprecated(contextId, rev, meta) -> "context",
      InstanceEvent.InstanceDeprecated(instId, rev, meta)  -> "instance"
    )

    "set the appropriate tags" in {
      forAll(mapping.toList) {
        case (ev, tag) =>
          adapter.toJournal(ev) shouldEqual Tagged(ev, Set(tag))
      }
    }

    "ignore other types" in {
      adapter.toJournal(schemaId) shouldEqual schemaId
    }

    "return an empty manifest" in {
      adapter.manifest(OrgEvent.OrgDeprecated(orgId, rev, meta)) shouldEqual ""
    }
  }

}
