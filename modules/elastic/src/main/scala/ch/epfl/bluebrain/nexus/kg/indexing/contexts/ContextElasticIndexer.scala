package ch.epfl.bluebrain.nexus.kg.indexing.contexts

import cats.MonadError
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextEvent, ContextId}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.indexing.{BaseElasticIndexer, ElasticIndexingSettings, PatchQuery}
import io.circe.Json
import journal.Logger

/**
  * Context incremental indexing logic that pushes data into an ElasticSearch indexer.
  *
  * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
  * @param settings the indexing settings
  * @tparam F the monadic effect type
  */
class ContextElasticIndexer[F[_]](client: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit F: MonadError[F, Throwable])
    extends BaseElasticIndexer[F](client, settings) {

  private val log        = Logger[this.type]
  private val versionKey = "version".qualifyAsString

  /**
    * Indexes the event by pushing it's json ld representation into the ElasticSearch indexer while also updating the
    * existing content.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: ContextEvent): F[Unit] = event match {
    case ContextCreated(id, rev, m, value) =>
      log.debug(s"Indexing 'ContextCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(false), published = Some(false))
      val data = value removeKeys ("links") deepMerge meta deepMerge Json.obj(
        createdAtTimeKey -> Json.fromString(m.instant.toString))
      client.create(event.id.toIndex(prefix), t, event.id.elasticId, data)

    case ContextUpdated(id, rev, m, value) =>
      log.debug(s"Indexing 'ContextUpdated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(false), published = Some(false))
      val query = PatchQuery.inverse(value deepMerge meta, createdAtTimeKey)
      client.update(event.id.toIndex(prefix),t, event.id.elasticId, query)

    case ContextPublished(id, rev, m) =>
      log.debug(s"Indexing 'ContextPublished' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = None, published = Some(true)) deepMerge Json.obj(
        publishedAtTimeKey                                                          -> Json.fromString(m.instant.toString))
      client.update(event.id.toIndex(prefix), t, event.id.elasticId, Json.obj("doc" -> meta))

    case ContextDeprecated(id, rev, m) =>
      log.debug(s"Indexing 'ContextDeprecated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(true), published = None)
      client.update(event.id.toIndex(prefix), t, event.id.elasticId, Json.obj("doc" -> meta))
  }

  private def buildMeta(id: ContextId,
                        rev: Long,
                        meta: Meta,
                        deprecated: Option[Boolean],
                        published: Option[Boolean]): Json = {
    val sharedObj = Json.obj(
      idKey            -> Json.fromString(id.qualifyAsString),
      revKey           -> Json.fromLong(rev),
      orgKey           -> Json.fromString(id.domainId.orgId.qualifyAsString),
      domainKey        -> Json.fromString(id.domainId.qualifyAsString),
      nameKey          -> Json.fromString(id.name),
      versionKey       -> Json.fromString(id.version.show),
      contextGroupKey  -> Json.fromString(id.contextName.qualifyAsString),
      updatedAtTimeKey -> Json.fromString(meta.instant.toString),
      rdfTypeKey       -> Json.fromString("Context".qualifyAsString)
    )

    val publishedObj = published
      .map(v => Json.obj(publishedKey -> Json.fromBoolean(v)))
      .getOrElse(Json.obj())

    val deprecatedObj = deprecated
      .map(v => Json.obj(deprecatedKey -> Json.fromBoolean(v)))
      .getOrElse(Json.obj())

    deprecatedObj deepMerge publishedObj deepMerge sharedObj
  }

}

object ContextElasticIndexer {

  /**
    * Constructs a context incremental indexer that pushes data into an ElasticSearch indexer.
    *
    * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
    * @param settings the indexing settings
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](client: ElasticClient[F], settings: ElasticIndexingSettings)(
      implicit F: MonadError[F, Throwable]): ContextElasticIndexer[F] =
    new ContextElasticIndexer[F](client, settings)
}
