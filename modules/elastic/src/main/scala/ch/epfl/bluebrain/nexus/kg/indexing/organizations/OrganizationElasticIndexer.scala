package ch.epfl.bluebrain.nexus.kg.indexing.organizations

import cats.MonadError
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.JsonOps._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.Contexts
import ch.epfl.bluebrain.nexus.kg.core.ld.JsonLdOps._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgEvent.{OrgCreated, OrgDeprecated, OrgUpdated}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgEvent, OrgId}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import ch.epfl.bluebrain.nexus.kg.indexing.{BaseElasticIndexer, ElasticIndexingSettings}
import io.circe.Json
import journal.Logger

/**
  * Organization incremental indexing logic that pushes data into an ElasticSearch indexer.
  *
  * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
  * @param contexts the context operation bundle
  * @param settings the indexing settings
  * @tparam F the monadic effect type
  */
class OrganizationElasticIndexer[F[_]](client: ElasticClient[F],
                                       contexts: Contexts[F],
                                       settings: ElasticIndexingSettings)(implicit F: MonadError[F, Throwable])
    extends BaseElasticIndexer[F](client, settings) {

  private val log = Logger[this.type]

  /**
    * Indexes the event by pushing it's json ld representation into the ElasticSearch indexer while also updating the
    * existing content.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: OrgEvent): F[Unit] = event match {
    case OrgCreated(id, rev, m, value) =>
      log.debug(s"Indexing 'OrgCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(false))
      createIndexIfNotExist(event.id).flatMap { _ =>
        contexts
          .resolve(value removeKeys ("links"))
          .map(_ deepMerge meta)
          .flatMap { json =>
            val jsonWithMeta = json deepMerge Json.obj(createdAtTimeKey -> m.instant.jsonLd)
            client.create(event.id.toIndex(prefix), t, event.id.elasticId, jsonWithMeta)
          }
      }

    case OrgUpdated(id, rev, m, value) =>
      log.debug(s"Indexing 'OrgUpdated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(false))
      contexts
        .resolve(value removeKeys ("links"))
        .map(_ deepMerge meta)
        .flatMap { json =>
          client.update(event.id.toIndex(prefix), t, event.id.elasticId, Json.obj("doc" -> json))
        }

    case OrgDeprecated(id, rev, m) =>
      log.debug(s"Indexing 'OrgDeprecated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, deprecated = Some(true))
      client.update(event.id.toIndex(prefix), t, event.id.elasticId, Json.obj("doc" -> meta))
  }

  private def buildMeta(id: OrgId, rev: Long, meta: Meta, deprecated: Option[Boolean]): Json = {
    val sharedObj = Json.obj(
      idKey            -> Json.fromString(id.qualifyAsString),
      revKey           -> Json.fromLong(rev),
      nameKey          -> Json.fromString(id.id),
      updatedAtTimeKey -> meta.instant.jsonLd,
      rdfTypeKey       -> "Organization".qualify.jsonLd
    )

    val deprecatedObj = deprecated
      .map(v => Json.obj(deprecatedKey -> Json.fromBoolean(v)))
      .getOrElse(Json.obj())

    deprecatedObj deepMerge sharedObj
  }
}

object OrganizationElasticIndexer {

  /**
    * Constructs a organization incremental indexer that pushes data into an ElasticSearch indexer.
    *
    * @param client   the ElasticSearch client to use for communicating with the ElasticSearch indexer
    * @param contexts the context operation bundle
    * @param settings the indexing settings
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](client: ElasticClient[F], contexts: Contexts[F], settings: ElasticIndexingSettings)(
      implicit F: MonadError[F, Throwable]): OrganizationElasticIndexer[F] =
    new OrganizationElasticIndexer[F](client, contexts, settings)
}
