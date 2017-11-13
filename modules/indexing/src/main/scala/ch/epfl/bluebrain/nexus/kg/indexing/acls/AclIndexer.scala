package ch.epfl.bluebrain.nexus.kg.indexing.acls

import akka.http.scaladsl.model.Uri
import cats.MonadError
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Event._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path.{Empty, Segment}
import ch.epfl.bluebrain.nexus.commons.iam.acls.{Event, Path, Permission}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.jsonld.UriJsonLDSupport._
import ch.epfl.bluebrain.nexus.kg.indexing.query.PatchQuery
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import io.circe.Json
import journal.Logger

/**
  * Acl incremental indexing logic that pushes data into an rdf triple store.
  *
  * @param client    the SPARQL client to use for communicating with the rdf triple store
  * @param settings  the indexing settings
  * @param F         a MonadError typeclass instance for ''F[_]''
  * @tparam F        the monadic effect type
  */
@SuppressWarnings(Array("UnusedMethodParameter"))
class AclIndexer[F[_]](client: SparqlClient[F], settings: AclIndexingSettings)(implicit F: MonadError[F, Throwable]) {

  private val log                                               = Logger[this.type]
  private val AclIndexingSettings(index, base, baseNs, baseVoc) = settings

  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](baseVoc)
  private val readKey                                               = "read".qualifyAsString

  /**
    * Indexes the event by pushing its json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: Event): F[Unit] = event match {
    case PermissionsCreated(path, acl, _) if isValidKg(path) =>
      val identities = acl.acl.filter(_.permissions(Permission.Read)).map(_.identity)
      add(path, identities)
    case PermissionsAdded(path, identity, permissions, _) if permissions(Permission.Read) && isValidKg(path) =>
      add(path, Set(identity))
    case PermissionsSubtracted(path, identity, permissions, _) if permissions(Permission.Read) && isValidKg(path) =>
      remove(path, identity)
    case PermissionsCleared(path, _) if isValidKg(path) =>
      clear(path)
    case PermissionsRemoved(path, identity, _) if isValidKg(path) =>
      remove(path, identity)
    case _ => F.pure(())
  }

  private def isValidKg(path: Path): Boolean = {
    val reverse = path.reverse
    reverse.head == "kg" && !reverse.tail.isEmpty
  }


  private def add(path: Path, identities: Set[Identity]): F[Unit] = {
    log.debug(s"Adding ACL indexing for path '$path' and identities '$identities'")
    val meta = buildMeta(path, identities)
    client.createGraph(index, qualifiedPath(path) qualifyWith base, meta)
  }

  private def remove(path: Path, identity: Identity): F[Unit] = {
    log.debug(s"Removing ACL indexing for path '$path' and identity '$identity'")
    val removeQuery = PatchQuery.exactMatch(qualifiedPath(path) qualifyAsStringWith base, readKey -> identity.id.id)
    client.delete(index, removeQuery)
  }

  private def clear(path: Path): F[Unit] = {
    log.debug(s"Clear ACLs indexing for path '$path'")
    client.clearGraph(index, qualifiedPath(path) qualifyWith base)
  }

  private def buildMeta(id: Path, value: Set[Identity]): Json = {
    Json.obj(
      idKey      -> Json.fromString(qualifiedPath(id) qualifyAsStringWith base),
      readKey    -> Json.arr(value.map(identity => Uri(identity.id.id).jsonLd).toSeq: _*),
      rdfTypeKey -> "Acl".qualify.jsonLd
    )
  }

  private def qualifiedPath(path: Path): String =
    addPrefix(path.reverse.tail.reverse).toString()

  private def addPrefix(path: Path): Path = {
    def inner(current: Path, depth: Int = 0): Int = {
      current match {
        case Empty             => depth
        case Segment(_, Empty) => depth
        case Segment(_, t)     => inner(t, depth + 1)
      }
    }
    val prefix = inner(path) match {
      case 0 => Path("organizations")
      case 1 => Path("domains")
      case 2 => Path("schemas")
      case 3 => Path("schemas")
      case _ => Path("data")
    }
    prefix ++ path
  }
}

object AclIndexer {
  def apply[F[_]](client: SparqlClient[F], settings: AclIndexingSettings)(implicit F: MonadError[F, Throwable]) =
    new AclIndexer(client, settings)
}
