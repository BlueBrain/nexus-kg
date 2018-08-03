package ch.epfl.bluebrain.nexus.kg.resources

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, AuthenticatedRef, GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.IamConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.rdf.Node
import ch.epfl.bluebrain.nexus.rdf.Node.{IriNode, Literal}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.service.http.Path
import ch.epfl.bluebrain.nexus.service.http.Path._
import ch.epfl.bluebrain.nexus.service.http.UriOps._

object syntax {

  final implicit class ResourceSyntax(resource: ResourceF[_, _, _]) {
    def isSchema: Boolean = resource.types.contains(nxv.Schema.value)
  }

  final implicit class IdentityIdSyntax(private val identity: Identity) extends AnyVal {
    def id(implicit iamConfig: IamConfig): IriNode = identity match {
      case UserRef(realm, sub)           => url"${iamConfig.baseUri.append("realms" / realm / "users" / sub)}"
      case GroupRef(realm, group)        => url"${iamConfig.baseUri.append("realms" / realm / "groups" / group)}"
      case AuthenticatedRef(Some(realm)) => url"${iamConfig.baseUri.append("realms" / realm / "authenticated")}"
      case AuthenticatedRef(_)           => url"${iamConfig.baseUri.append(Path("authenticated"))}"
      case Anonymous                     => url"${iamConfig.baseUri.append(Path("anonymous"))}"
    }
  }

  final implicit def toNode(instant: Instant): Node =
    Literal(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT), xsd.dateTime.value)
}
