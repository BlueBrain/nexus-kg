package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.IamConfig
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import scala.concurrent.duration._

class IdentityIdSyntaxSpec extends WordSpecLike with Matchers with Inspectors {
  private implicit val iamConfig = IamConfig("http://example.com", None, 30 seconds)

  "An IdentityIdSyntax" should {

    "convert an identity to an IriNode" in {
      val list = List(
        UserRef("r", "s")           -> url"http://example.com/realms/r/users/s",
        GroupRef("r", "g")          -> url"http://example.com/realms/r/groups/g",
        AuthenticatedRef(Some("r")) -> url"http://example.com/realms/r/authenticated",
        AuthenticatedRef(None)      -> url"http://example.com/authenticated",
        Anonymous                   -> url"http://example.com/anonymous"
      )
      forAll(list) {
        case (identity, iriNode) => identity.id shouldEqual iriNode
      }
    }
  }

}
