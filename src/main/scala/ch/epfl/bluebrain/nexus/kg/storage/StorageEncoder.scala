package ch.epfl.bluebrain.nexus.kg.storage

import cats.Id
import ch.epfl.bluebrain.nexus.commons.search.{QueryResult, QueryResults}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{DiskStorage, S3Storage}

import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.decoder.GraphDecoder.DecoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.GraphEncoder.EncoderResult
import ch.epfl.bluebrain.nexus.rdf.encoder.{GraphEncoder, RootNode}
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json

/**
  * Encoders for [[Storage]]
  */
object StorageEncoder {

  implicit val storageRootNode: RootNode[Storage] = s => IriNode(s.id)

  def json(qrsResolvers: QueryResults[Storage])(implicit enc: GraphEncoder[EncoderResult, QueryResults[Storage]],
                                                node: RootNode[QueryResults[Storage]]): DecoderResult[Json] =
    QueryResultEncoder.json(qrsResolvers, storageCtx mergeContext resourceCtx).map(_ addContext storageCtxUri)

  //TODO: Check if we want to allow everyone to see the volume or we protect it using permissions
  implicit val storageGraphEncoder: GraphEncoder[Id, Storage] = GraphEncoder {
    case (rootNode, storage: DiskStorage) =>
      val triples = mainTriples(storage) ++ Set[Triple]((storage.id, rdf.tpe, nxv.DiskStorage),
                                                        (storage.id, nxv.volume, storage.volume.toString))
      RootedGraph(rootNode, triples)
    case (rootNode, storage: S3Storage) =>
      val triples = mainTriples(storage) ++ Set[Triple]((storage.id, rdf.tpe, nxv.S3Storage),
                                                        (storage.id, rdf.tpe, nxv.Alpha))
      RootedGraph(rootNode, triples)
  }

  implicit val storageGraphEncoderEither: GraphEncoder[EncoderResult, Storage] = storageGraphEncoder.toEither

  implicit def qqStorageEncoder: GraphEncoder[Id, QueryResult[Storage]] =
    GraphEncoder { (rootNode, res) =>
      storageGraphEncoder(rootNode, res.source)
    }

  def mainTriples(storage: Storage): Set[Triple] = {
    val s = IriNode(storage.id)
    Set(
      (s, rdf.tpe, nxv.Storage),
      (s, nxv.deprecated, storage.deprecated),
      (s, nxv.rev, storage.rev),
      (s, nxv.default, storage.default),
      (s, nxv.algorithm, storage.algorithm)
    )
  }
}
