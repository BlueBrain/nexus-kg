package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.{Decoder, Encoder}
import ch.epfl.bluebrain.nexus.rdf.instances._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._

/**
  * Enumeration of storage reference types
  */
sealed trait StorageReference {

  /**
    *
    * @return the storage @id value
    */
  def id: AbsoluteIri

  /**
    * @return the storage revision
    */
  def rev: Long
}

object StorageReference {

  /**
    * Disk storage reference
    */
  final case class DiskStorageReference(id: AbsoluteIri, rev: Long) extends StorageReference

  /**
    * Remote Disk storage reference
    */
  final case class RemoteDiskStorageReference(id: AbsoluteIri, rev: Long) extends StorageReference

  /**
    * S3 Disk storage reference
    */
  final case class S3StorageReference(id: AbsoluteIri, rev: Long) extends StorageReference

  private implicit val config: Configuration = Configuration.default
    .withDiscriminator("@type")
    .copy(transformMemberNames = {
      case "id"  => "_resourceId"
      case "rev" => nxv.rev.prefix
      case other => other
    })

  implicit val storageReferenceEncoder: Encoder[StorageReference] = deriveEncoder[StorageReference]
  implicit val storageReferenceDecoder: Decoder[StorageReference] = deriveDecoder[StorageReference]

}
