package ch.epfl.bluebrain.nexus.kg.archives

import cats.Id
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.archives.Archive.ResourceDescription
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._

/**
  * Encoders for [[Archive]]
  */
object ArchiveEncoder {

  implicit val archiveRootNode: RootNode[Archive] = a => IriNode(a.resId.value)

  private def triplesFor(
      ss: IriOrBNode,
      project: Project,
      id: AbsoluteIri,
      rev: Option[Long],
      tag: Option[String],
      path: Option[Path]
  ): Set[Triple] = {
    Set[Triple]((ss, nxv.resourceId, id), (ss, nxv.project, project.show)) ++
      rev.map[Triple](r => (ss, nxv.rev, r)) ++
      tag.map[Triple](t => (ss, nxv.tag, t)) ++
      path.map[Triple](p => (ss, nxv.path, p.pctEncoded))
  }

  private def triplesFor(desc: ResourceDescription): (IriOrBNode, Set[Triple]) = {
    val ss = blank
    val triples = desc match {
      case Archive.File(id, project, rev, tag, path) =>
        triplesFor(ss, project, id, rev, tag, path) + ((ss, rdf.tpe, nxv.File): Triple)
      case Archive.Resource(id, project, rev, tag, originalSource, path) =>
        triplesFor(ss, project, id, rev, tag, path) ++ Set[Triple](
          (ss, nxv.originalSource, originalSource),
          (ss, rdf.tpe, nxv.Resource)
        )
    }
    ss -> triples
  }

  implicit val archiveGraphEncoder: GraphEncoder[Id, Archive] = GraphEncoder {
    case (rootNode, archive) =>
      val triples = archive.values.foldLeft(Set[Triple]((rootNode, rdf.tpe, nxv.Archive))) {
        (acc, resourceDescription) =>
          val (resourceId, triples) = triplesFor(resourceDescription)
          acc + ((rootNode, nxv.resources, resourceId)) ++ triples
      }
      RootedGraph(rootNode, triples)
  }

  implicit val archiveGraphEncoderEither: GraphEncoder[EncoderResult, Archive] = archiveGraphEncoder.toEither
}
