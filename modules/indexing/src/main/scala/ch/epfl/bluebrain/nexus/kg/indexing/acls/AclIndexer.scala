package ch.epfl.bluebrain.nexus.kg.indexing.acls

import cats.MonadError
import ch.epfl.bluebrain.nexus.commons.iam.acls.Event._
import ch.epfl.bluebrain.nexus.commons.iam.acls.{AccessControl, Event, Path, Permission}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient

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

  private val AclIndexingSettings(index, base, baseNs, baseVoc) = settings

  /**
    * Indexes the event by pushing its json ld representation into the rdf triple store while also updating the
    * existing triples.
    *
    * @param event the event to index
    * @return a Unit value in the ''F[_]'' context
    */
  final def apply(event: Event): F[Unit] = event match {
    case PermissionsCreated(path, acl, meta) =>
      acl.acl.map { case AccessControl(id, perms) if perms(Permission.Read) => add(path, id) }
      F.pure(())
    case PermissionsAdded(path, identity, permissions, meta) if permissions(Permission.Read) =>
      add(path, identity)
    case PermissionsSubtracted(path, identity, permissions, meta) if permissions(Permission.Read) =>
      remove(path, identity)
    case PermissionsCleared(path, meta) =>
      clear(path)
    case PermissionsRemoved(path, identity, meta) =>
      remove(path, identity)
    case _ => F.pure(())
  }

  // TODO: implement
  private def add(path: Path, identity: Identity): F[Unit] = ???

  private def remove(path: Path, identity: Identity): F[Unit] = ???

  private def clear(path: Path): F[Unit] = ???
}

object AclIndexer {
  def apply[F[_]](client: SparqlClient[F], settings: AclIndexingSettings)(implicit F: MonadError[F, Throwable]) =
    new AclIndexer(client, settings)
}
