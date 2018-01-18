package ch.epfl.bluebrain.nexus.kg.indexing.schemas

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.ld.JsonLdOps._
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaEvent, SchemaId}
import ch.epfl.bluebrain.nexus.kg.indexing.BaseSparqlIndexer
import ch.epfl.bluebrain.nexus.kg.indexing.query.PatchQuery
import io.circe.Json
import journal.Logger

/**
  * Schema incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param client   the SPARQL client to use for communicating with the rdf triple store
  * @param contexts the context operation bundle
  * @param settings the indexing settings
  * @tparam F       the monadic effect type
  */
class SchemaIndexer[F[_]](client: SparqlClient[F], contexts: Contexts[F], settings: SchemaIndexingSettings)(
    implicit F: MonadError[F, Throwable])
    extends BaseSparqlIndexer(settings.schemasBase, settings.nexusVocBase) {

  private val log                                         = Logger[this.type]
  private val SchemaIndexingSettings(index, _, baseNs, _) = settings
  private val versionKey                                  = "version".qualifyAsString

  /**
    * Indexes the event by pushing it's json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: SchemaEvent): F[Unit] = event match {
    case SchemaCreated(id, rev, m, value) =>
      log.debug(s"Indexing 'SchemaCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(false), published = Some(false))
      contexts
        .resolve(value removeKeys ("links"))
        .map(_ deepMerge meta)
        .flatMap { json =>
          client
            .createGraph(index, id qualifyWith baseNs, json deepMerge Json.obj(createdAtTimeKey -> m.instant.jsonLd))
        }

    case SchemaUpdated(id, rev, m, value) =>
      log.debug(s"Indexing 'SchemaUpdated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(false), published = Some(false))
      contexts
        .resolve(value removeKeys ("links"))
        .map(_ deepMerge meta)
        .flatMap { json =>
          val removeQuery = PatchQuery.inverse(id qualifyWith baseNs, createdAtTimeKey)
          client.patchGraph(index, id qualifyWith baseNs, removeQuery, json)
        }

    case SchemaPublished(id, rev, m) =>
      log.debug(s"Indexing 'SchemaPublished' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = None, published = Some(true)) deepMerge Json.obj(
        publishedAtTimeKey -> m.instant.jsonLd)
      val removeQuery = PatchQuery(id, id qualifyWith baseNs, revKey, publishedKey, updatedAtTimeKey)
      client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)

    case SchemaDeprecated(id, rev, m) =>
      log.debug(s"Indexing 'SchemaDeprecated' event for id '${id.show}'")
      val meta        = buildMeta(id, rev, m, deprecated = Some(true), published = None)
      val removeQuery = PatchQuery(id, id qualifyWith baseNs, revKey, deprecatedKey, updatedAtTimeKey)
      client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)
  }

  private def buildMeta(id: SchemaId,
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
      schemaGroupKey   -> id.schemaName.qualify.jsonLd,
      updatedAtTimeKey -> meta.instant.jsonLd,
      rdfTypeKey       -> "Schema".qualify.jsonLd
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

object SchemaIndexer {

  /**
    * Constructs a schema incremental indexer that pushes data into an rdf triple store.
    *
    * @param client   the SPARQL client to use for communicating with the rdf triple store
    * @param contexts  the context operation bundle
    * @param settings the indexing settings
    * @tparam F       the monadic effect type
    */
  final def apply[F[_]](client: SparqlClient[F], contexts: Contexts[F], settings: SchemaIndexingSettings)(
      implicit F: MonadError[F, Throwable]): SchemaIndexer[F] =
    new SchemaIndexer[F](client, contexts, settings)
}
