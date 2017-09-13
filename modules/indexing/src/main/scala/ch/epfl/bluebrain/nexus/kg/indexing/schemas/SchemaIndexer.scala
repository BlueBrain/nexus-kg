package ch.epfl.bluebrain.nexus.kg.indexing.schemas

import cats.instances.string._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaEvent._
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaEvent, SchemaId}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.PatchQuery
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import io.circe.Json
import journal.Logger

/**
  * Schema incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param client   the SPARQL client to use for communicating with the rdf triple store
  * @param settings the indexing settings
  * @tparam F       the monadic effect type
  */
class SchemaIndexer[F[_]](client: SparqlClient[F], settings: SchemaIndexingSettings) {

  private val log = Logger[this.type]
  private val SchemaIndexingSettings(index, base, baseNs, baseVoc) = settings

  private implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)
  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](baseVoc)

  private val idKey         = "@id"
  private val revKey        = "rev".qualifyAsString
  private val deprecatedKey = "deprecated".qualifyAsString
  private val publishedKey  = "published".qualifyAsString
  private val orgKey        = "organization".qualifyAsString
  private val domainKey     = "domain".qualifyAsString
  private val nameKey       = "schema".qualifyAsString
  private val versionKey    = "version".qualifyAsString

  /**
    * Indexes the event by pushing it's json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: SchemaEvent): F[Unit] = event match {
    case SchemaCreated(id, rev, value) =>
      log.debug(s"Indexing 'SchemaCreated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, deprecated = Some(false), published = Some(false))
      val data = value deepMerge meta
      client.createGraph(index, id qualifyWith baseNs, data)

    case SchemaUpdated(id, rev, value) =>
      log.debug(s"Indexing 'SchemaUpdated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, deprecated = Some(false), published = Some(false))
      val data = value deepMerge meta
      client.replaceGraph(index, id qualifyWith baseNs, data)

    case SchemaPublished(id, rev) =>
      log.debug(s"Indexing 'SchemaPublished' event for id '${id.show}'")
      val meta = buildMeta(id, rev, deprecated = None, published = Some(true))
      val removeQuery = PatchQuery(id, revKey, publishedKey)
      client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)

    case SchemaDeprecated(id, rev) =>
      log.debug(s"Indexing 'SchemaDeprecated' event for id '${id.show}'")
      val meta = buildMeta(id, rev, deprecated = Some(true), published = None)
      val removeQuery = PatchQuery(id, revKey, deprecatedKey)
      client.patchGraph(index, id qualifyWith baseNs, removeQuery, meta)
  }

  private def buildMeta(id: SchemaId, rev: Long, deprecated: Option[Boolean], published: Option[Boolean]): Json = {
    val sharedObj = Json.obj(
      idKey      -> Json.fromString(id.qualifyAsString),
      revKey     -> Json.fromLong(rev),
      orgKey     -> Json.fromString(id.domainId.orgId.id),
      domainKey  -> Json.fromString(id.domainId.id),
      nameKey    -> Json.fromString(id.name),
      versionKey -> Json.fromString(id.version.show))

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
    * @param settings the indexing settings
    * @tparam F       the monadic effect type
    */
  final def apply[F[_]](client: SparqlClient[F], settings: SchemaIndexingSettings): SchemaIndexer[F] =
    new SchemaIndexer[F](client, settings)
}