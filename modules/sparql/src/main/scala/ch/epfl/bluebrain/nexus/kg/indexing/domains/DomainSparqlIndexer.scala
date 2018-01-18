package ch.epfl.bluebrain.nexus.kg.indexing.domains

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Meta
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainEvent._
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainEvent, DomainId}
import ch.epfl.bluebrain.nexus.kg.core.ld.JsonLdOps._
import ch.epfl.bluebrain.nexus.kg.indexing.BaseSparqlIndexer
import ch.epfl.bluebrain.nexus.kg.indexing.query.PatchQuery
import io.circe.Json
import journal.Logger

/**
  * Domain incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param client   the SPARQL client to use for communicating with the rdf triple store
  * @param settings the indexing settings
  * @tparam F the monadic effect type
  */
class DomainSparqlIndexer[F[_]](client: SparqlClient[F], settings: DomainSparqlIndexingSettings)
    extends BaseSparqlIndexer(settings.domainBase, settings.nexusVocBase) {

  private val log                                               = Logger[this.type]
  private val DomainSparqlIndexingSettings(index, _, baseNs, _) = settings

  private val descriptionKey = "description".qualifyAsString

  /**
    * Indexes the event by pushing it's json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: DomainEvent): F[Unit] = event match {
    case DomainCreated(id, rev, m, description) =>
      log.debug(s"Indexing 'DomainCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, m, Some(description), deprecated = Some(false)) deepMerge Json.obj(
        createdAtTimeKey -> m.instant.jsonLd)
      client.createGraph(index, id qualifyWith baseNs, meta)

    case DomainDeprecated(id, rev, m) =>
      log.debug(s"Indexing 'DomainDeprecated' event for id '${id.show}'")
      val meta        = buildMeta(id, rev, m, None, deprecated = Some(true))
      val removeQuery = PatchQuery(id, id qualifyWith baseNs, revKey, deprecatedKey, updatedAtTimeKey)
      client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)
  }

  private def buildMeta(id: DomainId,
                        rev: Long,
                        meta: Meta,
                        description: Option[String],
                        deprecated: Option[Boolean]): Json = {
    val sharedObj = Json.obj(
      idKey            -> Json.fromString(id.qualifyAsString),
      revKey           -> Json.fromLong(rev),
      orgKey           -> id.orgId.qualify.jsonLd,
      nameKey          -> Json.fromString(id.id),
      updatedAtTimeKey -> meta.instant.jsonLd,
      rdfTypeKey       -> "Domain".qualify.jsonLd
    )

    val deprecatedObj = deprecated
      .map(v => Json.obj(deprecatedKey -> Json.fromBoolean(v)))
      .getOrElse(Json.obj())

    val descriptionObj = description
      .map(d => Json.obj(descriptionKey -> Json.fromString(d)))
      .getOrElse(Json.obj())

    deprecatedObj deepMerge descriptionObj deepMerge sharedObj
  }
}

object DomainSparqlIndexer {

  /**
    * Constructs a domain incremental indexer that pushes data into an rdf triple store.
    *
    * @param client   the SPARQL client to use for communicating with the rdf triple store
    * @param settings the indexing settings
    * @tparam F the monadic effect type
    */
  final def apply[F[_]](client: SparqlClient[F], settings: DomainSparqlIndexingSettings): DomainSparqlIndexer[F] =
    new DomainSparqlIndexer[F](client, settings)
}
