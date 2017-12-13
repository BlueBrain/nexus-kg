package ch.epfl.bluebrain.nexus.kg.indexing.acls

import akka.http.scaladsl.model.Uri
import cats.MonadError
import cats.instances.list._
import cats.instances.string._
import cats.syntax.functor._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Event._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path.{Empty, Segment}
import ch.epfl.bluebrain.nexus.commons.iam.acls.{Event, Path, Permission}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.JsonLDKeys._
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.jsonld.IndexJsonLdSupport._
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

  private val rootPathKey = "root"

  private val log                                                   = Logger[this.type]
  private val AclIndexingSettings(index, base, baseNs, baseVoc)     = settings
  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](baseVoc)

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

  private def isValidKg(path: Path): Boolean = path.reverse.head == "kg"

  private def isRootPath(path: Path): Boolean = path match {
    case Segment("kg", Empty) => true
    case _                    => false
  }

  private def add(path: Path, identities: Set[Identity]): F[Unit] = {
    F.sequence(qualifiedPaths(path) map { pathString =>
        log.debug(s"Adding ACL indexing for path '$pathString' and identities '$identities'")
        val meta = buildMeta(pathString, identities)
        client.createGraph(index, baseNs, meta)
      })
      .map(_ => ())
  }

  private def remove(path: Path, identity: Identity): F[Unit] = {
    F.sequence(qualifiedPaths(path) map { pathString =>
        log.debug(s"Removing ACL indexing for path '$pathString' and identity '$identity'")
        val removeQuery = PatchQuery.exactMatch(pathToQualifiedId(pathString), readKey -> identity.id.id)
        client.delete(index, removeQuery)
      })
      .map(_ => ())
  }

  private def clear(path: Path): F[Unit] = {
    F.sequence(qualifiedPaths(path) map { pathString =>
        log.debug(s"Clear ACLs indexing for path '$pathString'")
        val removeQuery = PatchQuery(pathToQualifiedId(pathString), baseNs, readKey)(Qualifier.configured[String](""))
        client.delete(index, removeQuery)
      })
      .map(_ => ())
  }

  private def buildMeta(path: String, value: Set[Identity]): Json =
    Json.obj(
      idKey   -> Json.fromString(pathToQualifiedId(path)),
      readKey -> Json.arr(value.map(identity => Uri(identity.id.show).jsonLd).toSeq: _*)
    )

  private def pathToQualifiedId(path: String): String =
    if (rootPathKey == path) path qualifyAsStringWith baseVoc
    else path qualifyAsStringWith base

  private def qualifiedPaths(path: Path): List[String] =
    if (isRootPath(path)) List(rootPathKey)
    else addPrefix(path.reverse.tail.reverse).map(_.toString())

  private def addPrefix(path: Path): List[Path] = {
    def inner(current: Path, depth: Int = 0): Int = {
      current match {
        case Empty             => depth
        case Segment(_, Empty) => depth
        case Segment(_, t)     => inner(t, depth + 1)
      }
    }
    val prefix = inner(path) match {
      case 0 => List(Path("organizations"))
      case 1 => List(Path("domains"))
      case 2 => List(Path("schemas"), Path("contexts"))
      case 3 => List(Path("schemas"), Path("contexts"))
      case _ => List(Path("data"))
    }
    prefix.map(_ ++ path)
  }
}

object AclIndexer {
  def apply[F[_]](client: SparqlClient[F], settings: AclIndexingSettings)(implicit F: MonadError[F, Throwable]) =
    new AclIndexer(client, settings)
}
