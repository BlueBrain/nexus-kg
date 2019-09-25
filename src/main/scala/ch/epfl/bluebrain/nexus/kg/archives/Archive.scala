package ch.epfl.bluebrain.nexus.kg.archives

import java.time.{Clock, Instant}

import cats.Monad
import cats.data.{EitherT, OptionT}
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.iam.client.types.{Identity, Permission}
import ch.epfl.bluebrain.nexus.kg.archives.Archive.ResourceDescription
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ArchivesConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.{nxv, nxva}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectLabel, Rejection, ResId}
import ch.epfl.bluebrain.nexus.kg.storage.AkkaSource
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.cursor.GraphCursor
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoder
import ch.epfl.bluebrain.nexus.rdf.encoder.NodeEncoderError.IllegalConversion
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Node, RootedGraph}

/**
  * Describes a set of resources
  */
final case class Archive(resId: ResId, created: Instant, createdBy: Identity, values: Set[ResourceDescription]) {

  def toTarIgnoreNotFound[F[_]: Effect](implicit fetchResource: FetchResource[F, ArchiveSource]): F[AkkaSource] =
    values.toList.traverse(fetchResource(_).value).map(results => TarFlow.write(results.flatten))

  def toTar[F[_]: Effect](implicit fetchResource: FetchResource[F, ArchiveSource]): OptionT[F, AkkaSource] =
    values.toList.map(fetchResource(_)).sequence.map(TarFlow.write(_))
}

object Archive {

  val write: Permission = Permission.unsafe("archives/write")

  /**
    * Enumeration of resource descriptions
    */
  sealed trait ResourceDescription extends Product with Serializable

  /**
    * Description of a file resource
    *
    * @param id             the unique identifier of the resource in a given project
    * @param project        the project where the resource belongs
    * @param rev            the optional revision of the resource
    * @param tag            the optional tag of the resource
    * @param path           the optional relative path on the tar bundle for the targeted resource
    */
  final case class File(
      id: AbsoluteIri,
      project: Project,
      rev: Option[Long],
      tag: Option[String],
      path: Option[Path]
  ) extends ResourceDescription

  /**
    * Description of a non file resource
    *
    * @param id             the unique identifier of the resource in a given project
    * @param project        the project where the resource belongs
    * @param rev            the optional revision of the resource
    * @param tag            the optional tag of the resource
    * @param originalSource a flag to decide whether the original payload or the payload with metadata and JSON-LD context are going to be fetched. Default = true
    * @param path           the optional relative path on the tar bundle for the targeted resource
    */
  final case class Resource(
      id: AbsoluteIri,
      project: Project,
      rev: Option[Long],
      tag: Option[String],
      originalSource: Boolean,
      path: Option[Path]
  ) extends ResourceDescription

  private type ProjectResolver[F[_]] = Option[ProjectLabel] => EitherT[F, Rejection, Project]

  private implicit val encoderPath: NodeEncoder[Path] = (node: Node) =>
    node
      .as[String]
      .flatMap(Path.rootless(_) match {
        case Right(path) if path.nonEmpty => Right(path)
        case _                            => Left(IllegalConversion(""))
      })

  private def resourceDescriptions[F[_]: Monad](mainId: AbsoluteIri, iter: Iterable[GraphCursor])(
      implicit projectResolver: ProjectResolver[F]
  ): EitherT[F, Rejection, Set[ResourceDescription]] = {

    def resourceDescription(c: GraphCursor): EitherT[F, Rejection, ResourceDescription] = {
      val result = for {
        id           <- c.downField(nxv.resourceId).focus.as[AbsoluteIri].onError(mainId.ref, "resourceId")
        tpe          <- c.downField(rdf.tpe).focus.as[AbsoluteIri].onError(id.ref, "@type")
        rev          <- c.downField(nxva.rev).focus.asOption[Long].onError(id.ref, nxva.rev.prefix)
        tag          <- c.downField(nxva.tag).focus.asOption[String].onError(id.ref, nxva.tag.prefix)
        projectLabel <- c.downField(nxva.project).focus.asOption[ProjectLabel].onError(id.ref, nxva.project.prefix)
        origSource   <- c.downField(nxv.originalSource).focus.as[Boolean](true).onError(id.ref, nxv.originalSource.prefix)
        path         <- c.downField(nxv.path).focus.asOption[Path].onError(id.ref, nxv.path.prefix)
      } yield (id, tpe, rev, tag, projectLabel, origSource, path)
      result match {
        case Right((id, _, Some(_), Some(_), _, _, _)) =>
          EitherT.leftT[F, ResourceDescription](
            InvalidResourceFormat(id.ref, "'tag' and 'rev' cannot be present at the same time."): Rejection
          )
        case Right((id, tpe, rev, tag, projectLabel, _, path)) if tpe == nxv.File.value =>
          projectResolver(projectLabel).map(project => File(id, project, rev, tag, path))
        case Right((id, tpe, rev, tag, projectLabel, originalSource, path)) if tpe == nxv.Resource.value =>
          projectResolver(projectLabel).map(project => Resource(id, project, rev, tag, originalSource, path))
        case Right((id, tpe, _, _, _, _, _)) =>
          EitherT.leftT[F, ResourceDescription](
            InvalidResourceFormat(
              id.ref,
              s"Invalid '@type' field '$tpe'. Recognized types are '${nxv.File.value}' and '${nxv.Resource.value}'."
            ): Rejection
          )
        case Left(rejection) => EitherT.leftT[F, ResourceDescription](rejection)
      }
    }

    iter.toList.foldM(Set.empty[ResourceDescription]) { (acc, innerCursor) =>
      resourceDescription(innerCursor).map(acc + _)
    }
  }

  /**
    * Attempts to constructs a [[Archive]] from the provided graph
    *
    * @param id      the resource identifier
    * @param graph   the graph
    * @param cache   the project cache from where to obtain the project references
    * @param project the project where the resource bundle is created
    * @return Right(archive) when successful and Left(rejection) when failed
    */
  final def apply[F[_]](id: AbsoluteIri, graph: RootedGraph)(
      implicit cache: ProjectCache[F],
      project: Project,
      F: Monad[F],
      config: ArchivesConfig,
      subject: Subject,
      clock: Clock
  ): EitherT[F, Rejection, Archive] = {

    implicit val projectResolver: ProjectResolver[F] = {
      case Some(label: ProjectLabel) => OptionT(cache.getBy(label)).toRight[Rejection](ProjectsNotFound(Set(label)))
      case None                      => EitherT.rightT[F, Rejection](project)
    }

    def duplicatedPathCheck(resources: Set[ResourceDescription]): EitherT[F, Rejection, Unit] = {
      val (duplicated, _) = resources.foldLeft((false, Set.empty[Path])) {
        case ((true, acc), _)                                => (true, acc)
        case ((_, acc), Resource(_, _, _, _, _, Some(path))) => (acc.contains(path), acc + path)
        case ((_, acc), File(_, _, _, _, Some(path)))        => (acc.contains(path), acc + path)
        case ((_, acc), _)                                   => (false, acc)
      }
      if (duplicated) EitherT.leftT[F, Unit](InvalidResourceFormat(id.ref, "Duplicated 'path' fields"): Rejection)
      else EitherT.rightT[F, Rejection](())
    }

    def maxResourcesCheck(resources: Set[ResourceDescription]): EitherT[F, Rejection, Unit] =
      if (resources.size > config.maxResources) {
        val msg = s"Too many resources. Maximum resources allowed: '${config.maxResources}'. Found: '${resources.size}'"
        EitherT.leftT[F, Unit](InvalidResourceFormat(id.ref, msg): Rejection)
      } else EitherT.rightT[F, Rejection](())

    val cursor = graph.cursor()
    for {
      resources <- resourceDescriptions[F](id, cursor.downField(nxv.resources).downArray)
      _         <- maxResourcesCheck(resources)
      _         <- duplicatedPathCheck(resources)
    } yield Archive(Id(project.ref, id), clock.instant, subject, resources)
  }
}
