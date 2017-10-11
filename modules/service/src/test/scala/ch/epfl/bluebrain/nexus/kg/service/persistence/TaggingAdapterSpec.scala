package ch.epfl.bluebrain.nexus.kg.service.persistence

import akka.persistence.journal.Tagged
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainEvent, DomainId}
import ch.epfl.bluebrain.nexus.kg.core.instances.{InstanceEvent, InstanceId}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgEvent, OrgId}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaEvent, SchemaId}
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class TaggingAdapterSpec extends WordSpecLike with Matchers with Inspectors {

  "A TaggingAdapter" should {

    val adapter = new TaggingAdapter()

    val id = "id"
    val orgId = OrgId(id)
    val domId = DomainId(orgId, id)
    val schemaId = SchemaId(domId, "name", Version(1, 1, 1))
    val instId = InstanceId(schemaId, id)
    val rev = 1L

    val mapping = Map(
      OrgEvent.OrgDeprecated(orgId, rev) -> "organization",
      DomainEvent.DomainDeprecated(domId, rev) -> "domain",
      SchemaEvent.SchemaDeprecated(schemaId, rev) -> "schema",
      InstanceEvent.InstanceDeprecated(instId, rev) -> "instance"
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
      adapter.manifest(OrgEvent.OrgDeprecated(orgId, rev)) shouldEqual ""
    }
  }

}
