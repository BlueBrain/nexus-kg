package ch.epfl.bluebrain.nexus.kg.core.contexts

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class ContextNameSpec extends WordSpecLike with Matchers with Inspectors {

  "A ContextName" should {
    val domId       = DomainId(OrgId("org"), "dom")
    val contextName = ContextName(domId, "name")

    "have a proper string representation" in {
      contextName.show shouldEqual s"org/dom/name"
    }

    "be properly converted into a contextId with a provided version" in {
      val version = Version(1, 0, 0)
      contextName.versioned(version) shouldEqual ContextId(domId, "name", version)
    }
  }
}
