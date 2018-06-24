package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.ddata.LWWRegister.Clock
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, LWWRegister, LWWRegisterKey}
import akka.pattern.ask
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task

import scala.concurrent.{ExecutionContext, Future}

/**
  * Project specific operations.
  */
trait Projects[F[_]] {

  /**
    * Looks up the deprecation state of the argument account.
    *
    * @param ref the account reference
    * @return Some(true) if the account is known to be deprecated, Some(false) if the account is known NOT to be
    *         deprecated and None if there's no information on the account state
    */
  def deprecated(ref: AccountRef): F[Option[Boolean]]

  /**
    * Looks up the deprecation state of the argument project.
    *
    * @param ref the project reference
    * @return Some(true) if the project is known to be deprecated, Some(false) if the project is known NOT to be
    *         deprecated and None if there's no information on the project state
    */
  def deprecated(ref: ProjectRef): F[Option[Boolean]]

  /**
    * Looks up the collection of defined resolvers for the argument project.
    *
    * @param ref the project reference
    * @return the collection of known resolvers configured for the argument project
    */
  def resolvers(ref: ProjectRef): F[Set[Resolver]]

  /**
    * Adds the resolver to the collection of project resolvers.
    *
    * @param ref       the project reference
    * @param resolver  the resolver to add
    * @param updateRev whether to update the resolver collection if the resolver provided has a higher revision than an
    *                  already existing element in the collection with the same id
    * @return true if the update was performed or false if the element was already in the set
    */
  def addResolver(ref: ProjectRef, resolver: Resolver, updateRev: Boolean): F[Boolean]

  /**
    * Removes the resolver identified by the argument id from the collection of project resolvers.
    *
    * @param ref       the project reference
    * @param id        the id of the resolver to remove
    * @param timestamp the instant used to merge the register value
    * @return true of the removal was performed or false of the element was not in the set
    */
  def removeResolver(ref: ProjectRef, id: AbsoluteIri, timestamp: Long): F[Boolean]

  /**
    * Either adds, updates or removes the argument resolver depending on its deprecation state, revision and the current
    * state of the register.
    *
    * @param ref      the project reference
    * @param resolver the resolver
    * @return true if an update has taken place, false otherwise
    */
  def applyResolver(ref: ProjectRef, resolver: Resolver): F[Boolean] =
    if (resolver.deprecated) removeResolver(ref, resolver.id, resolver.instant.toEpochMilli)
    else addResolver(ref, resolver, updateRev = true)

  /**
    * Looks up the collection of defined views for the argument project.
    *
    * @param ref the project reference
    * @return the collection of known views configured for the argument project
    */
  def views(ref: ProjectRef): F[Set[View]]
}

object Projects {

  private[async] def accountStateKey(ref: AccountRef): LWWRegisterKey[RevisionedValue[Boolean]] =
    LWWRegisterKey("account_state_" + ref.id)

  private[async] def projectStateKey(ref: ProjectRef): LWWRegisterKey[RevisionedValue[Boolean]] =
    LWWRegisterKey("project_state_" + ref.id)

  private[async] def resolverKey(ref: ProjectRef): LWWRegisterKey[TimestampedValue[Set[Resolver]]] =
    LWWRegisterKey("project_resolvers_" + ref.id)

  private[async] def viewKey(ref: ProjectRef): LWWRegisterKey[TimestampedValue[Set[View]]] =
    LWWRegisterKey("project_views_" + ref.id)

  /**
    * Constructs a ''Projects'' instance in a ''Future'' effect type.
    *
    * @param as the underlying actor system
    * @param tm timeout used for the lookup operations
    */
  def future()(implicit as: ActorSystem, tm: Timeout): Projects[Future] = new Projects[Future] {
    private val replicator                    = DistributedData(as).replicator
    private implicit val ec: ExecutionContext = as.dispatcher
    private implicit val node: Cluster        = Cluster(as)

    private implicit def tsClock[A]: Clock[TimestampedValue[A]] = TimestampedValue.timestampedValueClock

    override def deprecated(ref: AccountRef): Future[Option[Boolean]] =
      (replicator ? Get(accountStateKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => Some(g.get(accountStateKey(ref)).value.value)
        case NotFound(_, _)                       => None
      }

    override def deprecated(ref: ProjectRef): Future[Option[Boolean]] =
      (replicator ? Get(projectStateKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => Some(g.get(projectStateKey(ref)).value.value)
        case NotFound(_, _)                       => None
      }

    override def resolvers(ref: ProjectRef): Future[Set[Resolver]] =
      (replicator ? Get(resolverKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(resolverKey(ref)).value.value
        case NotFound(_, _)                       => Set.empty
      }

    override def addResolver(ref: ProjectRef, resolver: Resolver, updateRev: Boolean): Future[Boolean] = {
      val found = (r: Resolver) =>
        if (updateRev) r.id == resolver.id && r.rev >= resolver.rev
        else r.id == resolver.id

      resolvers(ref).flatMap { resolverSet =>
        if (resolverSet.exists(found)) Future.successful(false)
        else {
          val empty  = LWWRegister(TimestampedValue(0L, Set.empty[Resolver]))
          val value  = TimestampedValue(resolver.instant.toEpochMilli, resolverSet + resolver)
          val update = Update(resolverKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
          (replicator ? update).flatMap {
            case UpdateSuccess(LWWRegisterKey(_), _) =>
              Future.successful(true)
            case UpdateTimeout(LWWRegisterKey(_), _) =>
              Future.failed(OperationTimedOut("Timed out while waiting for add resolver quorum response"))
          }
        }
      }
    }

    override def removeResolver(ref: ProjectRef, id: AbsoluteIri, timestamp: Long): Future[Boolean] = {
      resolvers(ref).flatMap { resolverSet =>
        if (!resolverSet.exists(_.id == id)) Future.successful(false)
        else {
          val empty  = LWWRegister(TimestampedValue(0L, Set.empty[Resolver]))
          val value  = TimestampedValue(timestamp, resolverSet.filter(_.id != id))
          val update = Update(resolverKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
          (replicator ? update).flatMap {
            case UpdateSuccess(LWWRegisterKey(_), _) =>
              Future.successful(true)
            case UpdateTimeout(LWWRegisterKey(_), _) =>
              Future.failed(OperationTimedOut("Timed out while waiting for remove resolver quorum response"))
          }
        }
      }
    }

    override def views(ref: ProjectRef): Future[Set[View]] =
      (replicator ? Get(viewKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(viewKey(ref)).value.value
        case NotFound(_, _)                       => Set.empty
      }
  }

  /**
    * Constructs a ''Projects'' instance in a ''Task'' effect type.
    *
    * @param as the underlying actor system
    * @param tm timeout used for the lookup operations
    */
  def task()(implicit as: ActorSystem, tm: Timeout): Projects[Task] =
    new Projects[Task] {
      private val underlying = future()

      override def deprecated(ref: AccountRef): Task[Option[Boolean]] =
        Task.deferFuture(underlying.deprecated(ref))

      override def deprecated(ref: ProjectRef): Task[Option[Boolean]] =
        Task.deferFuture(underlying.deprecated(ref))

      override def resolvers(ref: ProjectRef): Task[Set[Resolver]] =
        Task.deferFuture(underlying.resolvers(ref))

      override def addResolver(ref: ProjectRef, resolver: Resolver, updateRev: Boolean): Task[Boolean] =
        Task.deferFuture(underlying.addResolver(ref, resolver, updateRev))

      override def removeResolver(ref: ProjectRef, id: AbsoluteIri, timestamp: Long): Task[Boolean] =
        Task.deferFuture(underlying.removeResolver(ref, id, timestamp))

      override def views(ref: ProjectRef): Task[Set[View]] =
        Task.deferFuture(underlying.views(ref))
    }
}
