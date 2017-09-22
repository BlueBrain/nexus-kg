package ch.epfl.bluebrain.nexus.kg.core.schemas

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class SchemaNameSpec extends WordSpecLike with Matchers with Inspectors {

  "A SchemaName" should {
    val domId = DomainId(OrgId("org"), "dom")
    val schemaName = SchemaName(domId, "name")

    "have a proper string representation" in {
      schemaName.show shouldEqual s"org/dom/name"
    }

    "be properly converted into a schemaId with a provided version" in {
      val version = Version(1,0,0)
      schemaName.versioned(version) shouldEqual SchemaId(domId, "name", version)
    }
  }
}