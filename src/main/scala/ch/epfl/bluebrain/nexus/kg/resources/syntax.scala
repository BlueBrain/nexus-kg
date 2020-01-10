package ch.epfl.bluebrain.nexus.kg.resources

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.UUID

import akka.http.scaladsl.model.{ContentType, Uri}
import akka.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID}
import cats.Monad
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.{ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.storage.Crypto
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Node.Literal
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.{Node, RootedGraph}
import ch.epfl.bluebrain.nexus.sourcing.projections.syntax._
import io.circe.{Decoder, Encoder}
import javax.crypto.SecretKey
import com.typesafe.scalalogging.Logger

import scala.util.{Success, Try}

object syntax {
  implicit class OffsetResourceSyntax(private val offset: Offset) extends AnyVal {

    def asInstant: Option[Instant] = offset match {
      case NoOffset | Sequence(_) => None
      case tm: TimeBasedUUID      => Some(tm.asInstant)
    }
  }

  implicit class ResIdSyntax(private val resId: ResId) extends AnyVal {
    def toGraphUri: Uri = (resId.value + "graph").toAkkaUri
  }

  implicit class EncoderResultSyntax[A](private val result: NodeEncoder.EncoderResult[A]) extends AnyVal {
    def onError(ref: Ref, field: String): Either[Rejection, A] =
      result.left.map(_ => InvalidResourceFormat(ref, s"'$field' field does not have the right format."): Rejection)
  }

  implicit val projectLabelEncoder: NodeEncoder[ProjectLabel] = node =>
    NodeEncoder.stringEncoder(node).flatMap { value =>
      value.trim.split("/") match {
        case Array(organization, project) => Right(ProjectLabel(organization, project))
        case _                            => Left(IllegalConversion("Expected a ProjectLabel, but found otherwise"))
      }
    }

  implicit val projectUuidEncoder: NodeEncoder[ProjectRef] = node =>
    NodeEncoder.stringEncoder(node).flatMap { value =>
      Try(UUID.fromString(value)) match {
        case Success(uuid) => Right(ProjectRef(uuid))
        case _             => Left(IllegalConversion("Expected a ProjectRef, but found otherwise"))
      }
    }

  implicit val encMediaType: Encoder[ContentType] =
    Encoder.encodeString.contramap(_.value)

  implicit val decMediaType: Decoder[ContentType] =
    Decoder.decodeString.emap(ContentType.parse(_).left.map(_.mkString("\n")))

  final implicit class ResourceSyntax(resource: ResourceF[_]) {
    def isSchema: Boolean = resource.types.contains(nxv.Schema.value)
  }

  final implicit def toNode(instant: Instant): Node =
    Literal(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT), xsd.dateTime.value)

  final implicit class ResourceUriSyntax(private val res: Resource)(implicit project: Project, http: HttpConfig) {
    def accessId: AbsoluteIri = AccessId(res.id.value, res.schema.iri)
  }

  final implicit class ResourceVUriSyntax(private val res: ResourceV)(implicit project: Project, http: HttpConfig) {
    def accessId: AbsoluteIri = AccessId(res.id.value, res.schema.iri)
  }

  final implicit class RootedGraphSyntaxMeta(private val graph: RootedGraph) extends AnyVal {

    /**
      * Removes the metadata triples from the rooted graph.
      *
      * @return a new [[RootedGraph]] without the metadata triples
      */
    def removeMetadata: RootedGraph = ResourceF.removeMetadata(graph)
  }

  final implicit class AclsSyntax(private val acls: AccessControlLists) extends AnyVal {

    /**
      * Checks if on the list of ACLs there are some which contains any of the provided ''identities'', ''perm'' in
      * the root path, the organization path or the project path.
      *
      * @param identities the list of identities to filter from the ''acls''
      * @param label      the organization and project label information to be used to generate the paths to filter
      * @param perm       the permission to filter
      * @return true if the conditions are met, false otherwise
      */
    def exists(identities: Set[Identity], label: ProjectLabel, perm: Permission): Boolean =
      acls.filter(identities).value.exists {
        case (path, v) =>
          (path == / || path == Segment(label.organization, /) || path == label.organization / label.value) &&
            v.value.permissions.contains(perm)
      }

    /**
      * Checks if on the list of ACLs there are some which contains any of the provided ''identities'', ''perm'' in
      * the root path or the organization path.
      *
      * @param identities the list of identities to filter from the ''acls''
      * @param label      the organization label information to be used to generate the paths to filter
      * @param perm       the permission to filter
      * @return true if the conditions are met, false otherwise
      */
    def exists(identities: Set[Identity], label: String, perm: Permission): Boolean =
      acls.filter(identities).value.exists {
        case (path, v) => (path == / || path == Segment(label, /)) && v.value.permissions.contains(perm)
      }

    /**
      * Checks if on the list of ACLs there are some which contain any of the provided ''identities'', ''perm'' in
      * the root path.
      *
      * @param identities the list of identities to filter from the ''acls''
      * @param perm       the permission to filter
      * @return true if the conditions are met, false otherwise
      */
    def existsOnRoot(identities: Set[Identity], perm: Permission): Boolean =
      acls.filter(identities).value.exists {
        case (path, v) =>
          path == / && v.value.permissions.contains(perm)
      }
  }

  final implicit class CallerSyntax(private val caller: Caller) extends AnyVal {

    /**
      * Evaluates if the provided ''project'' has the passed ''permission'' on the ''acls''.
      *
      * @param acls         the full list of ACLs
      * @param projectLabel the project to check for permissions validity
      * @param permission   the permission to filter
      */
    def hasPermission(acls: AccessControlLists, projectLabel: ProjectLabel, permission: Permission): Boolean =
      acls.exists(caller.identities, projectLabel, permission)

    /**
      * Filters from the provided ''projects'' the ones where the caller has the passed ''permission'' on the ''acls''.
      *
      * @param acls       the full list of ACLs
      * @param projects   the list of projects to check for permissions validity
      * @param permission the permission to filter
      * @return a set of [[ProjectLabel]]
      */
    def hasPermission(
        acls: AccessControlLists,
        projects: Set[ProjectLabel],
        permission: Permission
    ): Set[ProjectLabel] =
      projects.filter(hasPermission(acls, _, permission))
  }

  implicit class AbsoluteIriSyntax(private val iri: AbsoluteIri) extends AnyVal {
    def ref: Ref = Ref(iri)
  }

  implicit class ProjectSyntax(private val project: Project) extends AnyVal {

    /**
      * @return the [[ProjectLabel]] consisting of both the organization segment and the project segment
      */
    def projectLabel: ProjectLabel = ProjectLabel(project.organizationLabel, project.label)

    /**
      * @return the project reference
      */
    def ref: ProjectRef = ProjectRef(project.uuid)
  }

  implicit class CryptoSyntax(private val value: String) extends AnyVal {

    /**
      * Encrypts the ''value'' using the implicitly available ''key''
      */
    def encrypt(implicit key: SecretKey): String = Crypto.encrypt(key, value)

    /**
      * Decrypts the ''value'' using the implicitly available ''key''
      */
    def decrypt(implicit key: SecretKey): String = Crypto.decrypt(key, value)
  }

  implicit class IdentitiesSyntax(private val identities: Seq[Identity]) extends AnyVal {
    def foundInCaller(implicit caller: Caller): Boolean =
      identities.forall(caller.identities.contains)
  }

  implicit class PathProjectSyntax(private val path: Path) extends AnyVal {

    /**
      * Retrieves the available projects from the ''path''
      */
    def resolveProjects[F[_]](implicit projectCache: ProjectCache[F], log: Logger, F: Monad[F]): F[List[Project]] =
      path match {
        case `/` =>
          projectCache.list()
        case Segment(orgLabel, `/`) =>
          projectCache.list(orgLabel)
        case Segment(projectLabel, Slash(Segment(orgLabel, `/`))) =>
          projectCache.get(ProjectLabel(orgLabel, projectLabel)).map(_.map(List(_)).getOrElse(List.empty[Project]))
        case path =>
          F.pure(log.warn(s"Attempting to convert path '$path' to a project failed")) >>
            F.pure(List.empty[Project])
      }
  }
}
