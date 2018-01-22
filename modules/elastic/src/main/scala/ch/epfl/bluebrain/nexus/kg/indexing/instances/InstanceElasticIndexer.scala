package ch.epfl.bluebrain.nexus.kg.indexing.instances

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.iam.acls
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent._
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment._
import ch.epfl.bluebrain.nexus.kg.core.instances.{InstanceEvent, InstanceId}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.indexing.{BaseElasticIndexer, ElasticIndexingSettings, PatchQuery}
import io.circe.Json
import journal.Logger

/**
  * Instance incremental indexing logic that pushes data into an ElasticSearch indexer.
  *
  * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
  * @param contexts the context operation bundle
  * @param settings the indexing settings
  * @param F        a MonadError typeclass instance for ''F[_]''
  * @tparam F the monadic effect type
  */
class InstanceElasticIndexer[F[_]](client: ElasticClient[F], contexts: Contexts[F], settings: ElasticIndexingSettings)(
    implicit F: MonadError[F, Throwable])
    extends BaseElasticIndexer[F](client, settings) {

  private val log = Logger[this.type]

  // instance vocabulary
  private val uuidKey = "uuid".qualifyAsString

  // attachment vocabulary
  private val originalFileNameKey = "originalFileName".qualifyAsString
  private val mediaTypeKey        = "mediaType".qualifyAsString
  private val contentSizeKey      = "contentSize".qualifyAsString
  private val digestAlgoKey       = "digestAlgorithm".qualifyAsString
  private val digestKey           = "digest".qualifyAsString

  /**
    * Indexes the event by pushing it's json ld representation into the ElasticSearch indexer while also updating the
    * existing content.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: InstanceEvent): F[Unit] = event match {
    case InstanceCreated(id, rev, m, value) =>
      log.debug(s"Indexing 'InstanceCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = false)
      createIndexIfNotExist(event.id).flatMap { _ =>
        contexts
          .resolve(value removeKeys ("links"))
          .flatMap { json =>
            val combined = json deepMerge meta deepMerge Json.obj(
              createdAtTimeKey -> Json.fromString(m.instant.toString))
            client.create(event.id.toIndex(prefix), t, event.id.elasticId, combined)
          }
      }

    case InstanceUpdated(id, rev, m, value) =>
      log.debug(s"Indexing 'InstanceUpdated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = false)
      contexts
        .resolve(value removeKeys ("links"))
        .flatMap { json =>
          val query = PatchQuery.inverse(json deepMerge meta,
                                         createdAtTimeKey,
                                         originalFileNameKey,
                                         mediaTypeKey,
                                         contentSizeKey,
                                         digestAlgoKey,
                                         digestKey)
          client.update(event.id.toIndex(prefix), t, event.id.elasticId, query)
        }

    case InstanceDeprecated(id, rev, m) =>
      log.debug(s"Indexing 'InstanceDeprecated' event for id '${id.show}'")
      val meta = buildDeprecatedMeta(id, rev, m)
      client.update(event.id.toIndex(prefix), t, event.id.elasticId, Json.obj("doc" -> meta))

    case InstanceAttachmentCreated(id, rev, m, attachmentMeta) =>
      log.debug(s"Indexing 'InstanceAttachmentCreated' event for id '${id.show}'")
      val meta = buildAttachmentMeta(id, rev, attachmentMeta, m)
      client.update(event.id.toIndex(prefix), t, event.id.elasticId, Json.obj("doc" -> meta))

    case InstanceAttachmentRemoved(id, rev, m) =>
      log.debug(s"Indexing 'InstanceAttachmentRemoved' event for id '${id.show}'")
      val meta  = buildRevMeta(id, rev, m)
      val query = PatchQuery(meta, originalFileNameKey, mediaTypeKey, contentSizeKey, digestAlgoKey, digestKey)
      client.update(event.id.toIndex(prefix), t, event.id.elasticId, query)
  }

  private def buildMeta(id: InstanceId, rev: Long, meta: acls.Meta, deprecated: Boolean): Json =
    Json.obj(
      deprecatedKey  -> Json.fromBoolean(deprecated),
      orgKey         -> Json.fromString(id.schemaId.domainId.orgId.qualifyAsString),
      domainKey      -> Json.fromString(id.schemaId.domainId.qualifyAsString),
      schemaKey      -> Json.fromString(id.schemaId.qualifyAsString),
      schemaGroupKey -> Json.fromString(id.schemaId.schemaName.qualifyAsString),
      uuidKey        -> Json.fromString(id.id),
      rdfTypeKey     -> Json.fromString("Instance".qualifyAsString)
    ) deepMerge buildRevMeta(id, rev, meta)

  private def buildDeprecatedMeta(id: InstanceId, rev: Long, meta: acls.Meta): Json =
    Json.obj(
      deprecatedKey -> Json.fromBoolean(true),
    ) deepMerge buildRevMeta(id, rev, meta)

  private def buildAttachmentMeta(id: InstanceId, rev: Long, attachmentMeta: Attachment.Meta, meta: acls.Meta): Json = {
    val Meta(_, Info(originalFileName, contentType, Size(_, size), Digest(algorithm, digest))) = attachmentMeta
    Json.obj(
      originalFileNameKey -> Json.fromString(originalFileName),
      mediaTypeKey        -> Json.fromString(contentType),
      contentSizeKey      -> Json.fromLong(size),
      digestAlgoKey       -> Json.fromString(algorithm),
      digestKey           -> Json.fromString(digest)
    ) deepMerge buildRevMeta(id, rev, meta)
  }

  private def buildRevMeta(id: InstanceId, rev: Long, meta: acls.Meta): Json =
    Json.obj(idKey            -> Json.fromString(id.qualifyAsStringWith(base)),
             updatedAtTimeKey -> Json.fromString(meta.instant.toString),
             revKey           -> Json.fromLong(rev))

}

object InstanceElasticIndexer {

  /**
    * Constructs an instance incremental indexer that pushes data into an ElasticSearch indexer.
    *
    * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
    * @param contexts the context operation bundle
    * @param settings the indexing settings
    * @param F        a MonadError typeclass instance for ''F[_]''
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](client: ElasticClient[F], contexts: Contexts[F], settings: ElasticIndexingSettings)(
      implicit F: MonadError[F, Throwable]): InstanceElasticIndexer[F] =
    new InstanceElasticIndexer(client, contexts, settings)
}
