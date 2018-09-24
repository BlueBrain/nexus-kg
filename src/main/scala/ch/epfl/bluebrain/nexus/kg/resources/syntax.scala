package ch.epfl.bluebrain.nexus.kg.resources

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, AuthenticatedRef, GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{HttpConfig, IamConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{IriNode, IriOrBNode, Literal}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}
import ch.epfl.bluebrain.nexus.service.http.Path
import ch.epfl.bluebrain.nexus.service.http.UriOps._

object syntax {

  final implicit class ResourceSyntax(resource: ResourceF[_, _, _]) {
    def isSchema: Boolean = resource.types.contains(nxv.Schema.value)
  }

  final implicit class IdentityIdSyntax(private val identity: Identity) extends AnyVal {
    import ch.epfl.bluebrain.nexus.service.http.Path._
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

  final implicit class ResourceUriSyntax(private val res: Resource)(implicit wrapped: LabeledProject,
                                                                    http: HttpConfig) {
    def accessId: AbsoluteIri = AccessId(res.id.value, res.schema.iri)
  }

  final implicit class ResourceVUriSyntax(private val res: ResourceV)(implicit wrapped: LabeledProject,
                                                                      http: HttpConfig) {
    def accessId: AbsoluteIri = AccessId(res.id.value, res.schema.iri)
  }

  final implicit class GraphSyntaxMeta(private val graph: Graph) extends AnyVal {

    /**
      * Removes the metadata triples from the graph centered on the provided subject ''id''
      *
      * @param id the subject
      * @return a new [[Graph]] without the metadata triples
      */
    def removeMetadata(id: IriOrBNode): Graph = ResourceF.removeMetadata(graph, id)
  }

  final implicit class AclsSyntax(private val acls: FullAccessControlList) extends AnyVal {
    import ch.epfl.bluebrain.nexus.iam.client.types.Address._

    /**
      * Checks if on the list of ACLs there are some which contains any of the provided ''identities'', ''perms'' in
      * the root path, the account path or the project path.
      *
      * @param identities the list of identities to filter from the ''acls''
      * @param label      the account and project label information to be used to generate the paths to filter
      * @param perms      the permissions to filter
      * @return true if the conditions are met, false otherwise
      */
    def exists(identities: Set[Identity], label: ProjectLabel, perms: Permissions): Boolean =
      acls.acl.exists {
        case FullAccessControl(identity, path, permissions) =>
          identities.contains(identity) && permissions.containsAny(perms) &&
            (path == / || path == Address(label.account) || path == label.account / label.value)
      }
  }
}
