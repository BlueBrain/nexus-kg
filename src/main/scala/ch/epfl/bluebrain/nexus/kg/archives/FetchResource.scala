package ch.epfl.bluebrain.nexus.kg.archives

import java.time.Clock

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.data.OptionT
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.circe.syntax._
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller, Permission}
import ch.epfl.bluebrain.nexus.kg.archives.Archive.{File, Resource, ResourceDescription}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.resources.file.File.FileAttributes
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Files, Id, Resources}
import ch.epfl.bluebrain.nexus.kg.routes.JsonLDOutputFormat
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat.Compacted
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder.json
import ch.epfl.bluebrain.nexus.kg.storage.{AkkaSource, Storage}
import ch.epfl.bluebrain.nexus.kg.{urlEncode, KgError}
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import io.circe.{Json, Printer}

trait FetchResource[F[_], A] {
  def apply(value: ResourceDescription): OptionT[F, A]
}

object FetchResource {

  implicit def akkaSource[F[_]](
      implicit resources: Resources[F],
      files: Files[F],
      acls: AccessControlLists,
      caller: Caller,
      as: ActorSystem,
      F: Effect[F],
      clock: Clock
  ): FetchResource[F, ArchiveSource] =
    new FetchResource[F, ArchiveSource] {

      private val read: Permission = Permission.unsafe("resources/read")

      private def hasPermission(
          perm: Permission,
          project: Project
      )(implicit acls: AccessControlLists, caller: Caller): Boolean =
        acls.exists(caller.identities, project.projectLabel, perm)

      private val printer: Printer                          = Printer.spaces2.copy(dropNullValues = true)
      private implicit val outputFormat: JsonLDOutputFormat = Compacted

      private def toByteString(json: Json): ByteString =
        ByteString(printer.prettyByteBuffer(json.sortKeys(AppConfig.orderedKeys)))

      private def generatePath(optPath: Option[Path], defaultPath: => String): F[Path] =
        optPath match {
          case None =>
            Path.rootless(defaultPath) match {
              case Right(path) => F.pure(path)
              case Left(_)     => F.raiseError(KgError.InternalError(s"Invalid path generation from path '$defaultPath'"))
            }
          case Some(path) => F.pure(path)
        }

      private def generatePath(optPath: Option[Path], id: AbsoluteIri, project: Project): F[Path] =
        generatePath(optPath, s"${project.show}/${urlEncode(id.asUri)}.json")

      private def generatePath(optPath: Option[Path], attributes: FileAttributes, project: Project): F[Path] =
        generatePath(optPath, s"${project.show}/${attributes.filename}")

      private def fetchResource(r: Resource): OptionT[F, ArchiveSource] = {
        val id = Id(r.project.ref, r.id)
        if (hasPermission(read, r.project)) {
          OptionT(generatePath(r.path, r.id, r.project).map(Option.apply)).flatMap { path =>
            val jsonOptionF = (r.rev, r.tag, r.originalSource) match {
              case (Some(rev), _, true)  => resources.fetchSource(id, rev).toOption
              case (_, Some(tag), true)  => resources.fetchSource(id, tag).toOption
              case (_, _, true)          => resources.fetchSource(id).toOption
              case (Some(rev), _, false) => resources.fetch(id, rev)(r.project).subflatMap(json).toOption
              case (_, Some(tag), false) => resources.fetch(id, tag)(r.project).subflatMap(json).toOption
              case (None, None, false)   => resources.fetch(id)(r.project).subflatMap(json).toOption
              case _                     => OptionT.none[F, Json]
            }
            jsonOptionF.map { json =>
              val byteString = toByteString(json)
              ArchiveSource(byteString.size.toLong, path.pctEncoded, Source.single(byteString))
            }
          }
        } else
          OptionT.none[F, ArchiveSource]
      }

      private def fetchFile(r: File): OptionT[F, ArchiveSource] = {
        val resId = Id(r.project.ref, r.id)
        val fileSourceOptT = (r.rev, r.tag) match {
          case (Some(rev), _) => files.fetch(resId, rev).toOption
          case (_, Some(tag)) => files.fetch(resId, tag).toOption
          case (None, None)   => files.fetch(resId).toOption
          case _              => OptionT.none[F, (Storage, FileAttributes, AkkaSource)]
        }
        fileSourceOptT.flatMap {
          case (storage, attr, source) =>
            if (hasPermission(storage.readPermission, r.project))
              OptionT(generatePath(r.path, attr, r.project).map(Option.apply))
                .map(p => ArchiveSource(attr.bytes, p.pctEncoded, source))
            else
              OptionT.none[F, ArchiveSource]
        }
      }

      override def apply(value: ResourceDescription): OptionT[F, ArchiveSource] =
        value match {
          case file: File         => fetchFile(file)
          case resource: Resource => fetchResource(resource)
        }
    }
}
