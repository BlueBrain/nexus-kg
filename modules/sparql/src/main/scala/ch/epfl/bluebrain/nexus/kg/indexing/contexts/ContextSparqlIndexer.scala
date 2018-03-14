package ch.epfl.bluebrain.nexus.kg.indexing.contexts

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.sparql.client.{PatchStrategy, SparqlClient}
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextEvent._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextEvent, ContextId, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.ld.JsonLdOps._
import ch.epfl.bluebrain.nexus.kg.indexing.BaseSparqlIndexer
import io.circe.Json
import journal.Logger

/**
  * Domain incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param client   the SPARQL client to use for communicating with the rdf triple store
  * @param contexts the contexts operation bundle
  * @param settings the indexing settings
  */
class ContextSparqlIndexer[F[_]](client: SparqlClient[F],
                                 contexts: Contexts[F],
                                 settings: ContextSparqlIndexingSettings)(implicit F: MonadError[F, Throwable])
    extends BaseSparqlIndexer(settings.contextsBase, settings.nexusVocBase) {

  private val log        = Logger[this.type]
  private val baseNs     = settings.contextsBaseNs
  private val versionKey = "version".qualifyAsString

  /**
    * Indexes the event by pushing it's json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    */
  final def apply(event: ContextEvent): F[Unit] = event match {
    case ContextCreated(id, rev, m, value) =>
      log.debug(s"Indexing 'ContextCreated' event for id '${id.show}'")
      contexts.resolve(value removeKeys "links").flatMap { resolved =>
        val meta = buildMeta(id, rev, m, deprecated = Some(false), published = Some(false))
        val data = resolved deepMerge meta
        client.replace(id qualifyWith baseNs, data deepMerge Json.obj(createdAtTimeKey -> m.instant.jsonLd))
      }

    case ContextUpdated(id, rev, m, value) =>
      log.debug(s"Indexing 'ContextUpdated' event for id '${id.show}'")
      contexts.resolve(value removeKeys "links").flatMap { resolved =>
        val meta     = buildMeta(id, rev, m, deprecated = Some(false), published = Some(false))
        val data     = resolved deepMerge meta
        val strategy = PatchStrategy.removeButPredicates(Set(createdAtTimeKey))
        client.patch(id qualifyWith baseNs, data, strategy)
      }

    case ContextPublished(id, rev, m) =>
      log.debug(s"Indexing 'ContextPublished' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = None, published = Some(true)) deepMerge Json.obj(
        publishedAtTimeKey -> m.instant.jsonLd)
      val strategy = PatchStrategy.removePredicates(Set(revKey, publishedKey, updatedAtTimeKey))
      client.patch(id qualifyWith baseNs, meta, strategy)

    case ContextDeprecated(id, rev, m) =>
      log.debug(s"Indexing 'ContextDeprecated' event for id '${id.show}'")
      val meta     = buildMeta(id, rev, m, deprecated = Some(true), published = None)
      val strategy = PatchStrategy.removePredicates(Set(revKey, deprecatedKey, updatedAtTimeKey))
      client.patch(id qualifyWith baseNs, meta, strategy)
  }

  private def buildMeta(id: ContextId,
                        rev: Long,
                        meta: Meta,
                        deprecated: Option[Boolean],
                        published: Option[Boolean]): Json = {
    val sharedObj = Json.obj(
      idKey            -> Json.fromString(id.qualifyAsString),
      revKey           -> Json.fromLong(rev),
      orgKey           -> id.domainId.orgId.qualify.jsonLd,
      domainKey        -> id.domainId.qualify.jsonLd,
      nameKey          -> Json.fromString(id.name),
      versionKey       -> Json.fromString(id.version.show),
      contextGroupKey  -> id.contextName.qualify.jsonLd,
      updatedAtTimeKey -> meta.instant.jsonLd,
      rdfTypeKey       -> "Context".qualify.jsonLd
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

object ContextSparqlIndexer {
  final def apply[F[_]](client: SparqlClient[F], contexts: Contexts[F], settings: ContextSparqlIndexingSettings)(
      implicit F: MonadError[F, Throwable]): ContextSparqlIndexer[F] =
    new ContextSparqlIndexer[F](client, contexts, settings)
}
