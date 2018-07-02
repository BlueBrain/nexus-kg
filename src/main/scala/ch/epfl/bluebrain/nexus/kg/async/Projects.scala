package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.ddata.LWWRegister.Clock
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, LWWRegister, LWWRegisterKey}
import akka.pattern.ask
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
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
    * Looks up the state of the argument account.
    *
    * @param ref the account reference
    * @return Some(account) if there is an account with state and None if there's no information on the account state
    */
  def account(ref: AccountRef): F[Option[Account]]

  /**
    * Adds an account.
    *
    * @param ref       the account reference
    * @param account   the account to add
    * @param updateRev whether to update an existing account if the provided account has a higher revision than an
    *                  already existing element with the same id
    * @return true if the update was performed or false if the element was already present
    */
  def addAccount(ref: AccountRef, account: Account, updateRev: Boolean): F[Boolean]

  /**
    * Looks up the state of the argument project.
    *
    * @param ref the project reference
    * @return Some(project) if there is a project with state and None if there's no information on the project state
    */
  def project(ref: ProjectRef): F[Option[Project]]

  /**
    * Adds a project.
    *
    * @param ref       the project reference
    * @param project   the project to add
    * @param updateRev whether to update an existing project if the provided project has a higher revision than an
    *                  already existing element with the same id
    * @return true if the update was performed or false if the element was already present
    */
  def addProject(ref: ProjectRef, project: Project, updateRev: Boolean): F[Boolean]

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
    * @param instant   the instant used to merge the register value
    * @param updateRev whether to update the resolver collection if the resolver provided has a higher revision than an
    *                  already existing element in the collection with the same id
    * @return true if the update was performed or false if the element was already in the set
    */
  def addResolver(ref: ProjectRef, resolver: Resolver, instant: Instant, updateRev: Boolean): F[Boolean]

  /**
    * Removes the resolver identified by the argument id from the collection of project resolvers.
    *
    * @param ref     the project reference
    * @param id      the id of the resolver to remove
    * @param instant the instant used to merge the register value
    * @return true of the removal was performed or false of the element was not in the set
    */
  def removeResolver(ref: ProjectRef, id: AbsoluteIri, instant: Instant): F[Boolean]

  /**
    * Either adds, updates or removes the argument resolver depending on its deprecation state, revision and the current
    * state of the register.
    *
    * @param ref      the project reference
    * @param resolver the resolver
    * @param instant  the instant used to merge the register value
    * @return true if an update has taken place, false otherwise
    */
  def applyResolver(ref: ProjectRef, resolver: Resolver, instant: Instant): F[Boolean] =
    if (resolver.deprecated) removeResolver(ref, resolver.id, instant)
    else addResolver(ref, resolver, instant, updateRev = true)

  /**
    * Looks up the collection of defined views for the argument project.
    *
    * @param ref the project reference
    * @return the collection of known views configured for the argument project
    */
  def views(ref: ProjectRef): F[Set[View]]

  /**
    * Adds the view to the collection of project views.
    *
    * @param ref       the project reference
    * @param view      the view to add
    * @param instant   the instant used to merge the register value
    * @param updateRev whether to update the view collection if the view provided has a higher revision than an
    *                  already existing element in the collection with the same id
    * @return true if the update was performed or false if the element was already in the set
    */
  def addView(ref: ProjectRef, view: View, instant: Instant, updateRev: Boolean): F[Boolean]

  /**
    * Removes the view identified by the argument id from the collection of project views.
    *
    * @param ref     the project reference
    * @param id      the id of the view to remove
    * @param instant the instant used to merge the register value
    * @return true of the removal was performed or false of the element was not in the set
    */
  def removeView(ref: ProjectRef, id: AbsoluteIri, instant: Instant): F[Boolean]

  /**
    * Either adds, updates or removes the argument view depending on its deprecation state, revision and the current
    * state of the register.
    *
    * @param ref     the project reference
    * @param view    the view
    * @param instant the instant used to merge the register value
    * @return true if an update has taken place, false otherwise
    */
  def applyView(ref: ProjectRef, view: View, instant: Instant): F[Boolean] =
    if (view.deprecated) removeView(ref, view.id, instant)
    else addView(ref, view, instant, updateRev = true)
}

object Projects {

  private[async] def accountKey(ref: AccountRef): LWWRegisterKey[RevisionedValue[Option[Account]]] =
    LWWRegisterKey("account_state_" + ref.id)

  private[async] def projectKey(ref: ProjectRef): LWWRegisterKey[RevisionedValue[Option[Project]]] =
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

    override def account(ref: AccountRef): Future[Option[Account]] =
      (replicator ? Get(accountKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(accountKey(ref)).value.value
        case NotFound(_, _)                       => None
      }

    override def addAccount(ref: AccountRef, ac: Account, updateRev: Boolean): Future[Boolean] = {
      def update() = {
        val empty  = LWWRegister(RevisionedValue[Option[Account]](0L, None))
        val value  = RevisionedValue[Option[Account]](ac.rev, Some(ac))
        val update = Update(accountKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
        (replicator ? update).flatMap(handleBooleanUpdate("Timed out while waiting for add project quorum response"))
      }
      account(ref).flatMap {
        case None                                   => update()
        case Some(p) if updateRev && ac.rev > p.rev => update()
        case _                                      => Future.successful(false)

      }
    }

    override def project(ref: ProjectRef): Future[Option[Project]] =
      (replicator ? Get(projectKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(projectKey(ref)).value.value
        case NotFound(_, _)                       => None
      }

    override def addProject(ref: ProjectRef, proj: Project, updateRev: Boolean): Future[Boolean] = {
      def update() = {
        val empty  = LWWRegister(RevisionedValue[Option[Project]](0L, None))
        val value  = RevisionedValue[Option[Project]](proj.rev, Some(proj))
        val update = Update(projectKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
        (replicator ? update).flatMap(handleBooleanUpdate("Timed out while waiting for add project quorum response"))
      }
      project(ref).flatMap {
        case None                                     => update()
        case Some(p) if updateRev && proj.rev > p.rev => update()
        case _                                        => Future.successful(false)

      }
    }

    override def resolvers(ref: ProjectRef): Future[Set[Resolver]] =
      (replicator ? Get(resolverKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(resolverKey(ref)).value.value
        case NotFound(_, _)                       => Set.empty
      }

    override def addResolver(
        ref: ProjectRef,
        resolver: Resolver,
        instant: Instant,
        updateRev: Boolean
    ): Future[Boolean] = {
      val found = (r: Resolver) =>
        if (updateRev) r.id == resolver.id && r.rev >= resolver.rev
        else r.id == resolver.id

      resolvers(ref).flatMap { resolverSet =>
        if (resolverSet.exists(found)) Future.successful(false)
        else {
          val empty  = LWWRegister(TimestampedValue(0L, Set.empty[Resolver]))
          val value  = TimestampedValue(instant.toEpochMilli, resolverSet + resolver)
          val update = Update(resolverKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
          (replicator ? update).flatMap(handleBooleanUpdate("Timed out while waiting for add resolver quorum response"))
        }
      }
    }

    override def removeResolver(ref: ProjectRef, id: AbsoluteIri, instant: Instant): Future[Boolean] = {
      resolvers(ref).flatMap { resolverSet =>
        if (!resolverSet.exists(_.id == id)) Future.successful(false)
        else {
          val empty  = LWWRegister(TimestampedValue(0L, Set.empty[Resolver]))
          val value  = TimestampedValue(instant.toEpochMilli, resolverSet.filter(_.id != id))
          val update = Update(resolverKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
          (replicator ? update).flatMap(
            handleBooleanUpdate("Timed out while waiting for remove resolver quorum response"))
        }
      }
    }

    override def views(ref: ProjectRef): Future[Set[View]] =
      (replicator ? Get(viewKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(viewKey(ref)).value.value
        case NotFound(_, _)                       => Set.empty
      }

    override def addView(ref: ProjectRef, view: View, instant: Instant, updateRev: Boolean): Future[Boolean] = {
      val found = (v: View) =>
        if (updateRev) v.id == view.id && v.rev >= view.rev
        else v.id == view.id

      views(ref).flatMap { viewSet =>
        if (viewSet.exists(found)) Future.successful(false)
        else {
          val empty  = LWWRegister(TimestampedValue(0L, Set.empty[View]))
          val value  = TimestampedValue(instant.toEpochMilli, viewSet + view)
          val update = Update(viewKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
          (replicator ? update).flatMap(handleBooleanUpdate("Timed out while waiting for add view quorum response"))
        }
      }
    }

    override def removeView(ref: ProjectRef, id: AbsoluteIri, instant: Instant): Future[Boolean] = {
      views(ref).flatMap { viewSet =>
        if (!viewSet.exists(_.id == id)) Future.successful(false)
        else {
          val empty  = LWWRegister(TimestampedValue(0L, Set.empty[View]))
          val value  = TimestampedValue(instant.toEpochMilli, viewSet.filter(_.id != id))
          val update = Update(viewKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
          (replicator ? update).flatMap(handleBooleanUpdate("Timed out while waiting for remove view quorum response"))
        }
      }
    }

    private def handleBooleanUpdate(timeoutMsg: String): PartialFunction[Any, Future[Boolean]] = {
      case UpdateSuccess(LWWRegisterKey(_), _) =>
        Future.successful(true)
      case UpdateTimeout(LWWRegisterKey(_), _) =>
        Future.failed(OperationTimedOut(timeoutMsg))
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

      override def account(ref: AccountRef): Task[Option[Account]] =
        Task.deferFuture(underlying.account(ref))

      override def addAccount(ref: AccountRef, account: Account, updateRev: Boolean): Task[Boolean] =
        Task.deferFuture(underlying.addAccount(ref, account, updateRev))

      override def project(ref: ProjectRef): Task[Option[Project]] =
        Task.deferFuture(underlying.project(ref))

      override def addProject(ref: ProjectRef, project: Project, updateRev: Boolean): Task[Boolean] =
        Task.deferFuture(underlying.addProject(ref, project, updateRev))

      override def resolvers(ref: ProjectRef): Task[Set[Resolver]] =
        Task.deferFuture(underlying.resolvers(ref))

      override def addResolver(
          ref: ProjectRef,
          resolver: Resolver,
          instant: Instant,
          updateRev: Boolean
      ): Task[Boolean] =
        Task.deferFuture(underlying.addResolver(ref, resolver, instant, updateRev))

      override def removeResolver(ref: ProjectRef, id: AbsoluteIri, instant: Instant): Task[Boolean] =
        Task.deferFuture(underlying.removeResolver(ref, id, instant))

      override def views(ref: ProjectRef): Task[Set[View]] =
        Task.deferFuture(underlying.views(ref))

      override def addView(ref: ProjectRef, view: View, instant: Instant, updateRev: Boolean): Task[Boolean] =
        Task.deferFuture(underlying.addView(ref, view, instant, updateRev))

      override def removeView(ref: ProjectRef, id: AbsoluteIri, instant: Instant): Task[Boolean] =
        Task.deferFuture(underlying.removeView(ref, id, instant))
    }
}
