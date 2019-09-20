package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Clock

import akka.actor.ActorSystem
import cats.data.EitherT
import cats.effect.Effect
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.rdf.syntax._
import ch.epfl.bluebrain.nexus.commons.shacl.ShaclEngine
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, Caller}
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.archives.ArchiveEncoder._
import ch.epfl.bluebrain.nexus.kg.archives.{Archive, ArchiveCache}
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.Materializer
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound.notFound
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.apache.jena.rdf.model.Model

class Archives[F[_]](
    implicit cache: ArchiveCache[F],
    resources: Resources[F],
    files: Files[F],
    system: ActorSystem,
    materializer: Materializer[F],
    config: AppConfig,
    projectCache: ProjectCache[F],
    clock: Clock,
    F: Effect[F]
) {

  /**
    * Creates an archive.
    *
    * @param source the source representation in JSON-LD
    * @return either a rejection or the resource representation in the F context
    */
  def create(source: Json)(implicit project: Project, subject: Subject): RejOrResource[F] =
    materializer(source.addContext(archiveCtxUri)).flatMap {
      case (id, Value(_, _, graph)) => create(Id(project.ref, id), graph, source)
    }

  /**
    * Creates an archive.
    *
    * @param id     the id of the resource
    * @param source the source representation in JSON-LD
    * @return either a rejection or the resource representation in the F context
    */
  def create(id: ResId, source: Json)(implicit project: Project, subject: Subject): RejOrResource[F] =
    materializer(source.addContext(archiveCtxUri), id.value).flatMap {
      case Value(_, _, graph) => create(id, graph, source)
    }

  private def create(id: ResId, graph: RootedGraph, source: Json)(
      implicit project: Project,
      subject: Subject
  ): RejOrResource[F] = {
    val typedGraph = RootedGraph(id.value, graph.triples + ((id.value, rdf.tpe, nxv.Archive): Triple))
    val types      = typedGraph.rootTypes.map(_.value)
    for {
      _       <- validateShacl(typedGraph)
      archive <- Archive(id.value, typedGraph)
      _       <- cache.put(archive).toRight(ResourceAlreadyExists(id.ref): Rejection)
    } yield
    // format: off
      ResourceF(id, 1L, types, false, Map.empty, None, archive.created, archive.created, archive.createdBy, archive.createdBy, archiveRef, source)
    // format: on
  }

  private def validateShacl(data: RootedGraph): EitherT[F, Rejection, Unit] = {
    val model: CId[Model] = data.as[Model]()
    ShaclEngine(model, archiveSchemaModel, validateShapes = false, reportDetails = true) match {
      case Some(r) if r.isValid() => EitherT.rightT(())
      case Some(r)                => EitherT.leftT(InvalidResource(archiveRef, r))
      case _ =>
        EitherT(
          F.raiseError(InternalError(s"Unexpected error while attempting to validate schema '$archiveSchemaUri'"))
        )
    }
  }

  private def fetchArchive(id: ResId): RejOrArchive[F] =
    cache.get(id).toRight(notFound(id.ref, schema = Some(archiveRef)))

  /**
    * Fetches the archive.
    *
    * @param id the id of the collection source
    * @return either a rejection or the bytestring source in the F context
    */
  def fetchArchive(
      id: ResId,
      ignoreNotFound: Boolean
  )(implicit acls: AccessControlLists, caller: Caller): RejOrAkkaSource[F] =
    fetchArchive(id: ResId).flatMap { archive =>
      if (ignoreNotFound) EitherT.right(archive.toTarIgnoreNotFound[F])
      else archive.toTar[F].toRight(ArchiveElementNotFound: Rejection)
    }

  /**
    * Fetches the archive resource.
    *
    * @param id the id of the collection source
    * @return either a rejection or the resourceV in the F context
    */
  def fetch(id: ResId): RejOrResourceV[F] =
    fetchArchive(id).flatMap {
      case a @ Archive(resId, created, createdBy, _) =>
        val source = Json.obj().addContext(archiveCtxUri)
        val ctx    = archiveCtx.contextValue
        val eitherValue: EitherT[F, Rejection, ResourceF.Value] =
          EitherT
            .fromEither[F](a.asGraph[EncoderResult](resId.value).map(Value(source, ctx, _)))
            .leftSemiflatMap(e => Rejection.fromMarshallingErr[F](resId.value, e))
        eitherValue.map { value =>
          // format: off
          ResourceF(resId, 1L, Set(nxv.Archive), false, Map.empty, None, created, created, createdBy, createdBy, archiveRef, value)
          // format: on
        }
    }
}

object Archives {
  final def apply[F[_]: Effect: ArchiveCache: ProjectCache: Materializer](
      resources: Resources[F],
      files: Files[F]
  )(implicit system: ActorSystem, config: AppConfig, clock: Clock): Archives[F] = {
    implicit val r = resources
    implicit val f = files
    new Archives
  }
}
