package ch.epfl.bluebrain.nexus.kg.resources

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.UUID

import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types.Address.Segment
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, AuthenticatedRef, GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{HttpConfig, IamConfig}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{IriNode, IriOrBNode, Literal}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Node}
import ch.epfl.bluebrain.nexus.service.http.Path
import ch.epfl.bluebrain.nexus.service.http.UriOps._

import scala.util.{Success, Try}

object syntax {

  implicit val projectLabelEncoder: NodeEncoder[ProjectLabel] = node =>
    NodeEncoder.stringEncoder(node).flatMap { value =>
      value.trim.split("/") match {
        case Array(account, project) => Right(ProjectLabel(account, project))
        case _                       => Left(IllegalConversion("Expected a ProjectLabel, but found otherwise"))
      }
  }

  implicit val projectUuidEncoder: NodeEncoder[ProjectRef] = node =>
    NodeEncoder.stringEncoder(node).flatMap { value =>
      Try(UUID.fromString(value)) match {
        case Success(_) => Right(ProjectRef(value))
        case _          => Left(IllegalConversion("Expected a ProjectRef, but found otherwise"))
      }
  }

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

  final implicit class CallerSyntax(private val caller: Caller) extends AnyVal {

    /**
      * Evaluates if the provided ''project'' has some of the passed ''permissions'' on the ''acls''.
      *
      * @param acls         the full list of ACLs
      * @param projectLabel the project to check for permissions validity
      * @param permissions  the permissions to filter
      */
    def hasProjectPermission(acls: FullAccessControlList,
                             projectLabel: ProjectLabel,
                             permissions: Permissions): Boolean = {

      def verify(identity: Identity, perms: Permissions): Boolean =
        caller.identities.contains(identity) && perms.containsAny(permissions)

      val (org, proj) = projectLabel.account -> projectLabel.value
      acls.acl.exists {
        case FullAccessControl(ident, Segment(`proj`, Segment(`org`, Address./)), perms) => verify(ident, perms)
        case FullAccessControl(ident, Segment(`org`, Address./), perms)                  => verify(ident, perms)
        case FullAccessControl(ident, Address./, perms)                                  => verify(ident, perms)
        case _                                                                           => false
      }
    }

    /**
      * Filters from the provided ''projects'' the ones where the caller has some of the passed ''permissions'' on the ''acls''.
      *
      * @param acls        the full list of ACLs
      * @param projects    the list of projects to check for permissions validity
      * @param permissions the permissions to filter
      * @return a set of [[ProjectLabel]]
      */
    def hasPermission(acls: FullAccessControlList,
                      projects: Set[ProjectLabel],
                      permissions: Permissions): Set[ProjectLabel] =
      projects.filter(hasProjectPermission(acls, _, permissions))
  }
}
