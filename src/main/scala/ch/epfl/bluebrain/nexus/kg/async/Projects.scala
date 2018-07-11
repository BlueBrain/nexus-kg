package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.ddata.LWWRegister.Clock
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, LWWRegister, LWWRegisterKey}
import akka.pattern.ask
import akka.util.Timeout
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectLabel, ProjectRef}
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
    * Deprecates an account.
    *
    * @param ref the account reference
    * @param rev the account revision
    * @return true if the deprecation was performed or false otherwise
    */
  def deprecateAccount(ref: AccountRef, rev: Long): F[Boolean]

  /**
    * Looks up the state of the argument project.
    *
    * @param ref the project reference
    * @return Some(project) if there is a project with state and None if there's no information on the project state
    */
  def project(ref: ProjectRef): F[Option[Project]]

  /**
    * Looks up the state of the argument project.
    *
    * @param label the project label
    * @return Some(project) if there is a project with state and None if there's no information on the project state
    */
  def project(label: ProjectLabel): F[Option[Project]]

  /**
    * Looks up the state of the argument project ref.
    *
    * @param label the project label
    * @return Some(projectRef) if there is a project reference with state and None if there's no information on the project reference state
    */
  def projectRef(label: ProjectLabel): F[Option[ProjectRef]]

  /**
    * Adds a project.
    *
    * @param ref        the project reference
    * @param accountRef the account reference
    * @param project    the project to add
    * @param updateRev  whether to update an existing project if the provided project has a higher revision than an
    *                   already existing element with the same id
    * @return true if the update was performed or false if the element was already present
    */
  def addProject(ref: ProjectRef, accountRef: AccountRef, project: Project, updateRev: Boolean): F[Boolean]

  /**
    * Deprecates a project.
    *
    * @param ref the project reference
    * @param rev the project revision
    * @return true if the deprecation was performed or false otherwise
    */
  def deprecateProject(ref: ProjectRef, rev: Long): F[Boolean]

  /**
    * Looks up the collection of defined resolvers for the argument project.
    *
    * @param ref the project reference
    * @return the collection of known resolvers configured for the argument project
    */
  def resolvers(ref: ProjectRef): F[Set[Resolver]]

  /**
    * Looks up the collection of defined resolvers for the argument project.
    *
    * @param label the project label
    * @return the collection of known resolvers configured for the argument project
    */
  def resolvers(label: ProjectLabel): F[Set[Resolver]]

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
    * Looks up the collection of defined views for the argument project.
    *
    * @param label the project label
    * @return the collection of known views configured for the argument project
    */
  def views(label: ProjectLabel): F[Set[View]]

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

  private[async] def projectSegmentKey(ref: ProjectLabel): LWWRegisterKey[RevisionedValue[Option[ProjectRef]]] =
    LWWRegisterKey("project_segment_" + ref.show)

  private[async] def accountSegmentInverseKey(ref: AccountRef): LWWRegisterKey[RevisionedValue[Option[String]]] =
    LWWRegisterKey("account_segment_" + ref.id)

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

    private def update(ref: AccountRef, ac: Account) = {

      def updateAccount() = {
        val empty  = LWWRegister(RevisionedValue[Option[Account]](0L, None))
        val value  = RevisionedValue[Option[Account]](ac.rev, Some(ac))
        val update = Update(accountKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
        (replicator ? update).flatMap(handleBooleanUpdate("Timed out while waiting for add project quorum response"))
      }

      def updateAccountLabel() = {
        val empty  = LWWRegister(RevisionedValue[Option[String]](0L, None))
        val value  = RevisionedValue[Option[String]](ac.rev, Some(ac.label))
        val update = Update(accountSegmentInverseKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
        (replicator ? update).flatMap(
          handleBooleanUpdate("Timed out while waiting for adding account label quorum response"))
      }

      updateAccount().withFilter(_ == true).flatMap(_ => updateAccountLabel())
    }

    override def account(ref: AccountRef): Future[Option[Account]] =
      getOrElse(accountKey(ref), none[Account])

    private def accountSegment(ref: AccountRef): Future[Option[String]] =
      getOrElse(accountSegmentInverseKey(ref), none[String])

    override def addAccount(ref: AccountRef, ac: Account, updateRev: Boolean): Future[Boolean] =
      account(ref).flatMap {
        case None                                   => update(ref, ac)
        case Some(a) if updateRev && ac.rev > a.rev => update(ref, ac)
        case _                                      => Future.successful(false)
      }

    override def deprecateAccount(ref: AccountRef, rev: Long): Future[Boolean] =
      account(ref).flatMap {
        case Some(a) if !a.deprecated && rev > a.rev => update(ref, a.copy(rev = rev, deprecated = true))
        case _                                       => Future.successful(false)
      }

    private def update(ref: ProjectRef, proj: Project): Future[Boolean] = {
      val empty  = LWWRegister(RevisionedValue[Option[Project]](0L, None))
      val value  = RevisionedValue[Option[Project]](proj.rev, Some(proj))
      val update = Update(projectKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
      (replicator ? update).flatMap(handleBooleanUpdate("Timed out while waiting for add project quorum response"))
    }

    private def update(ref: ProjectRef, accountRef: AccountRef, proj: Project): Future[Boolean] =
      update(ref, proj).withFilter(_ == true).flatMap { _ =>
        accountSegment(accountRef).flatMap {
          case Some(accountL) =>
            val empty = LWWRegister(RevisionedValue[Option[ProjectRef]](0L, None))
            val value = RevisionedValue[Option[ProjectRef]](proj.rev, Some(ref))
            val update = Update(projectSegmentKey(ProjectLabel(accountL, proj.label)),
                                empty,
                                WriteMajority(tm.duration))(_.withValue(value))
            (replicator ? update).flatMap(
              handleBooleanUpdate("Timed out while waiting for add project quorum response"))
          case _ => Future.successful(false)
        }
      }

    override def project(ref: ProjectRef): Future[Option[Project]] =
      getOrElse(projectKey(ref), none[Project])

    override def projectRef(label: ProjectLabel): Future[Option[ProjectRef]] =
      getOrElse(projectSegmentKey(label), none[ProjectRef])

    override def project(label: ProjectLabel): Future[Option[Project]] =
      projectRef(label).flatMap {
        case Some(ref) => project(ref)
        case _         => Future(None)
      }

    override def addProject(ref: ProjectRef,
                            accountRef: AccountRef,
                            proj: Project,
                            updateRev: Boolean): Future[Boolean] =
      project(ref).flatMap {
        case None                                     => update(ref, accountRef, proj)
        case Some(p) if updateRev && proj.rev > p.rev => update(ref, accountRef, proj)
        case _                                        => Future.successful(false)
      }

    override def deprecateProject(ref: ProjectRef, rev: Long): Future[Boolean] =
      project(ref).flatMap {
        case Some(p) if !p.deprecated && rev > p.rev => update(ref, p.copy(rev = rev, deprecated = true))
        case _                                       => Future.successful(false)
      }

    override def resolvers(ref: ProjectRef): Future[Set[Resolver]] =
      getOrElse(resolverKey(ref), Set.empty)

    override def resolvers(label: ProjectLabel): Future[Set[Resolver]] =
      projectRef(label).flatMap {
        case Some(ref) => resolvers(ref)
        case _         => Future(Set.empty)
      }

    private def getOrElse[T, K <: RegisteredValue[T]](f: => LWWRegisterKey[K], default: => T): Future[T] =
      (replicator ? Get(f, ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(f).value.value
        case NotFound(_, _)                       => default
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
      getOrElse(viewKey(ref), Set.empty[View])

    override def views(label: ProjectLabel): Future[Set[View]] =
      projectRef(label).flatMap {
        case Some(ref) => views(ref)
        case _         => Future(Set.empty)
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

    private def none[A]: Option[A] = None
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

      override def resolvers(label: ProjectLabel): Task[Set[Resolver]] =
        Task.deferFuture(underlying.resolvers(label))

      override def views(label: ProjectLabel): Task[Set[View]] =
        Task.deferFuture(underlying.views(label))

      override def projectRef(label: ProjectLabel): Task[Option[ProjectRef]] =
        Task.deferFuture(underlying.projectRef(label))

      override def project(label: ProjectLabel): Task[Option[Project]] =
        Task.deferFuture(underlying.project(label))

      override def account(ref: AccountRef): Task[Option[Account]] =
        Task.deferFuture(underlying.account(ref))

      override def addAccount(ref: AccountRef, account: Account, updateRev: Boolean): Task[Boolean] =
        Task.deferFuture(underlying.addAccount(ref, account, updateRev))

      override def deprecateAccount(ref: AccountRef, rev: Long): Task[Boolean] =
        Task.deferFuture(underlying.deprecateAccount(ref, rev))

      override def project(ref: ProjectRef): Task[Option[Project]] =
        Task.deferFuture(underlying.project(ref))

      override def addProject(ref: ProjectRef,
                              accountRef: AccountRef,
                              project: Project,
                              updateRev: Boolean): Task[Boolean] =
        Task.deferFuture(underlying.addProject(ref, accountRef, project, updateRev))

      override def deprecateProject(ref: ProjectRef, rev: Long): Task[Boolean] =
        Task.deferFuture(underlying.deprecateProject(ref, rev))

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
