package ch.epfl.bluebrain.nexus.kg.indexing.instances

import akka.http.scaladsl.model.Uri
import cats.MonadError
import cats.instances.string._
import cats.syntax.show._
import cats.syntax.functor._
import cats.syntax.flatMap._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent._
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment._
import ch.epfl.bluebrain.nexus.kg.core.instances.{InstanceEvent, InstanceId}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.PatchQuery
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import io.circe.Json
import journal.Logger

/**
  * Instance incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param client   the SPARQL client to use for communicating with the rdf triple store
  * @param settings the indexing settings
  * @param F        a MonadError typeclass instance for ''F[_]''
  * @tparam F       the monadic effect type
  */
class InstanceIndexer[F[_]](client: SparqlClient[F], settings: InstanceIndexingSettings)
                           (implicit F: MonadError[F, Throwable]) {

  private val log = Logger[this.type]
  private val InstanceIndexingSettings(index, base, baseNs, baseVoc) = settings

  private implicit val instanceIdQualifier: ConfiguredQualifier[InstanceId] = Qualifier.configured[InstanceId](base)
  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](baseVoc)

  // instance vocabulary
  private val idKey         = "@id"
  private val revKey        = "rev".qualifyAsString
  private val deprecatedKey = "deprecated".qualifyAsString
  private val orgKey        = "organization".qualifyAsString
  private val domainKey     = "domain".qualifyAsString
  private val schemaKey     = "schema".qualifyAsString
  private val versionKey    = "version".qualifyAsString
  private val uuidKey       = "uuid".qualifyAsString

  // attachment vocabulary
  private val originalFileNameKey = "originalFileName".qualifyAsString
  private val contentTypeKey      = "contentType".qualifyAsString
  private val sizeKey             = "size".qualifyAsString
  private val digestAlgoKey       = "digestAlgorithm".qualifyAsString
  private val digestKey           = "digest".qualifyAsString

  /**
    * Indexes the event by pushing it's json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: InstanceEvent): F[Unit] = event match {
    case InstanceCreated(id, rev, value) =>
      log.debug(s"Indexing 'InstanceCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, deprecated = false)
      val data = value deepMerge meta
      client.createGraph(index, id qualifyWith baseNs, data)

    case InstanceUpdated(id, rev, value) =>
      log.debug(s"Indexing 'InstanceUpdated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, deprecated = false)
      val data = value deepMerge meta
      client.replaceGraph(index, id qualifyWith baseNs, data)

    case InstanceDeprecated(id, rev) =>
      log.debug(s"Indexing 'InstanceDeprecated' event for id '${id.show}'")
      val meta = buildDeprecatedMeta(id, rev)
      val removeQuery = PatchQuery(id, revKey, deprecatedKey)
      client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)

    case InstanceAttachmentCreated(id, rev, attachmentMeta) =>
      log.debug(s"Indexing 'InstanceAttachmentCreated' event for id '${id.show}'")
      val meta = buildAttachmentMeta(id, rev, attachmentMeta)
      val removeQuery = PatchQuery(id, revKey)
      for {
        _ <- client.patchGraph(index, id qualifyWith baseNs, removeQuery, buildRevMeta(id, rev))
        _ <- client.replaceGraph(index, s"${id qualifyWith baseNs}/attachment", meta)
      } yield ()

    case InstanceAttachmentRemoved(id, rev) =>
      log.debug(s"Indexing 'InstanceAttachmentRemoved' event for id '${id.show}'")
      val meta = buildRevMeta(id, rev)
      val removeQuery = PatchQuery(id, revKey)
      for {
        _ <- client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)
        _ <- client.clearGraph(index, attachmentGraph(id))
      } yield ()
  }

  private def buildMeta(id: InstanceId, rev: Long, deprecated: Boolean): Json =
    Json.obj(
      deprecatedKey -> Json.fromBoolean(deprecated),
      orgKey        -> Json.fromString(id.schemaId.domainId.orgId.id),
      domainKey     -> Json.fromString(id.schemaId.domainId.id),
      schemaKey     -> Json.fromString(id.schemaId.name),
      versionKey    -> Json.fromString(id.schemaId.version.show),
      uuidKey       -> Json.fromString(id.id)
    ) deepMerge buildRevMeta(id, rev)

  private def buildDeprecatedMeta(id: InstanceId, rev: Long): Json =
    Json.obj(deprecatedKey -> Json.fromBoolean(true)) deepMerge buildRevMeta(id, rev)

  private def buildAttachmentMeta(id: InstanceId, rev: Long, attachmentMeta: Attachment.Meta): Json = {
    val Meta(_, Info(originalFileName, contentType, Size(_, size), Digest(algorithm, digest))) = attachmentMeta
    Json.obj(
      originalFileNameKey -> Json.fromString(originalFileName),
      contentTypeKey      -> Json.fromString(contentType),
      sizeKey             -> Json.fromLong(size),
      digestAlgoKey       -> Json.fromString(algorithm),
      digestKey           -> Json.fromString(digest)
    ) deepMerge buildRevMeta(id, rev)
  }

  private def buildRevMeta(id: InstanceId, rev: Long): Json =
    Json.obj(
      idKey  -> Json.fromString(id.qualifyAsStringWith(base)),
      revKey -> Json.fromLong(rev))

  private def attachmentGraph(id: InstanceId): Uri =
    s"${id qualifyWith baseNs}/attachment"
}

object InstanceIndexer {
  /**
    * Constructs an instance incremental indexer that pushes data into an rdf triple store.
    *
    * @param client   the SPARQL client to use for communicating with the rdf triple store
    * @param settings the indexing settings
    * @param F        a MonadError typeclass instance for ''F[_]''
    * @tparam F       the monadic effect type
    */
  final def apply[F[_]](client: SparqlClient[F], settings: InstanceIndexingSettings)
                       (implicit F: MonadError[F, Throwable]): InstanceIndexer[F] =
    new InstanceIndexer(client, settings)
}