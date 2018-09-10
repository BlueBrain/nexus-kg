package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.ddata.LWWRegister.Clock
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, LWWRegister, LWWRegisterKey}
import akka.pattern.ask
import akka.util.Timeout
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.RevisionedId
import ch.epfl.bluebrain.nexus.kg.RevisionedId._
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.resolve._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import journal.Logger
import monix.eval.Task

import scala.concurrent.{ExecutionContext, Future}

/**
  * Contract for the distributed cache that keeps in-memory metadata and content
  * of the resources indexed by the service.
  */
trait DistributedCache[F[_]] {

  /**
    * Looks up the state of the argument account.
    *
    * @param ref the account reference
    * @return Some(account) if there is an account with state and None if there's no information on the account state
    */
  def account(ref: AccountRef): F[Option[Account]]

  /**
    * Looks up the state of the argument account ref.
    *
    * @param ref the project reference
    * @return Some(accountRef) if there is an account reference with state and None if there's no information on the account reference state
    */
  def accountRef(ref: ProjectRef): F[Option[AccountRef]]

  /**
    * Adds an account.
    *
    * @param ref       the account reference
    * @param account   the account to add
    */
  def addAccount(ref: AccountRef, account: Account): F[Unit]

  /**
    * Deprecates an account.
    *
    * @param ref the account reference
    * @param rev the account revision
    */
  def deprecateAccount(ref: AccountRef, rev: Long): F[Unit]

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
    */
  def addProject(ref: ProjectRef, accountRef: AccountRef, project: Project): F[Unit]

  /**
    * Deprecates a project.
    *
    * @param ref        the project reference
    * @param accountRef the account reference
    * @param rev        the project revision
    */
  def deprecateProject(ref: ProjectRef, accountRef: AccountRef, rev: Long): F[Unit]

  /**
    * Looks up the projects belonging to the argument account.
    *
    * @param ref the account reference
    * @return the collection of project references belonging to this account
    */
  def projects(ref: AccountRef): F[Set[ProjectRef]]

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
    */
  def addResolver(ref: ProjectRef, resolver: Resolver): F[Unit]

  /**
    * Removes the resolver identified by the argument id from the collection of project resolvers.
    *
    * @param ref     the project reference
    * @param id      the id of the resolver to remove
    * @param rev     revision of the deprecated resolver
    */
  def removeResolver(ref: ProjectRef, id: AbsoluteIri, rev: Long): F[Unit]

  /**
    * Either adds, updates or removes the argument resolver depending on its deprecation state, revision and the current
    * state of the register.
    *
    * @param ref      the project reference
    * @param resolver the resolver
    */
  def applyResolver(ref: ProjectRef, resolver: Resolver): F[Unit] =
    if (resolver.deprecated) removeResolver(ref, resolver.id, resolver.rev)
    else addResolver(ref, resolver)

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
    */
  def addView(ref: ProjectRef, view: View): F[Unit]

  /**
    * Removes the view identified by the argument id from the collection of project views.
    *
    * @param ref the project reference
    * @param id  the id of the view to remove
    * @param rev revision of the deprecated view
    */
  def removeView(ref: ProjectRef, id: AbsoluteIri, rev: Long): F[Unit]

  /**
    * Either adds, updates or removes the argument view depending on its deprecation state, revision and the current
    * state of the register.
    *
    * @param ref     the project reference
    * @param view    the view
    */
  def applyView(ref: ProjectRef, view: View): F[Unit] =
    if (view.deprecated) removeView(ref, view.id, view.rev)
    else addView(ref, view)
}

object DistributedCache {

  private[async] def accountKey(ref: AccountRef): LWWRegisterKey[RevisionedValue[Option[Account]]] =
    LWWRegisterKey("account_state_" + ref.id)

  private[async] def accountRefKey(ref: ProjectRef): LWWRegisterKey[RevisionedValue[Option[AccountRef]]] =
    LWWRegisterKey("account_key_state_" + ref.id)

  private[async] def projectKey(ref: ProjectRef): LWWRegisterKey[RevisionedValue[Option[Project]]] =
    LWWRegisterKey("project_state_" + ref.id)

  private[async] def projectSegmentKey(ref: ProjectLabel): LWWRegisterKey[RevisionedValue[Option[ProjectRef]]] =
    LWWRegisterKey("project_segment_" + ref.show)

  private[async] def accountSegmentInverseKey(ref: AccountRef): LWWRegisterKey[RevisionedValue[Option[String]]] =
    LWWRegisterKey("account_segment_" + ref.id)

  private[async] def accountProjectsKey(ref: AccountRef): LWWRegisterKey[RevisionedValue[Set[ProjectRef]]] =
    LWWRegisterKey("account_projects_" + ref.id)

  private[async] def projectResolversKey(ref: ProjectRef): LWWRegisterKey[RevisionedValue[Set[Resolver]]] =
    LWWRegisterKey("project_resolvers_" + ref.id)

  private[async] def projectViewsKey(ref: ProjectRef): LWWRegisterKey[RevisionedValue[Set[View]]] =
    LWWRegisterKey("project_views_" + ref.id)

  /**
    * Constructs a [[DistributedCache]] instance in a [[Future]] effect type.
    *
    * @param as the underlying actor system
    * @param tm timeout used for the lookup operations
    */
  def future()(implicit as: ActorSystem, tm: Timeout): DistributedCache[Future] = new DistributedCache[Future] {

    private val log = Logger[this.type]

    private val replicator                    = DistributedData(as).replicator
    private implicit val ec: ExecutionContext = as.dispatcher
    private implicit val node: Cluster        = Cluster(as)

    private implicit def rvClock[A]: Clock[RevisionedValue[A]] = RevisionedValue.revisionedValueClock

    private def update(ref: AccountRef, ac: Account) = {

      def updateAccount() = {
        val empty  = LWWRegister(RevisionedValue[Option[Account]](0L, None))
        val value  = RevisionedValue[Option[Account]](ac.rev, Some(ac))
        val update = Update(accountKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
        (replicator ? update).flatMap(handleUpdate(s"update account ${ac.label}"))
      }

      def updateAccountLabel() = {
        val empty  = LWWRegister(RevisionedValue[Option[String]](0L, None))
        val value  = RevisionedValue[Option[String]](ac.rev, Some(ac.label))
        val update = Update(accountSegmentInverseKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
        (replicator ? update).flatMap(handleUpdate(s"update account label for ${ac.label}"))
      }

      updateAccount().flatMap(_ => updateAccountLabel())
    }

    override def account(ref: AccountRef): Future[Option[Account]] =
      getOrElse(accountKey(ref), none[Account])

    override def accountRef(ref: ProjectRef): Future[Option[AccountRef]] =
      getOrElse(accountRefKey(ref), none[AccountRef])

    private def addAccountRef(ref: ProjectRef, accRef: AccountRef, rev: Long): Future[Unit] = {
      accountRef(ref).flatMap {
        case Some(_) => Future.successful(())
        case _ =>
          val empty  = LWWRegister(RevisionedValue[Option[AccountRef]](0L, None))
          val value  = RevisionedValue[Option[AccountRef]](rev, Some(accRef))
          val update = Update(accountRefKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
          (replicator ? update).flatMap(handleUpdate(s"update account label for ${accRef.id}"))
      }
    }

    private def accountSegment(ref: AccountRef): Future[Option[String]] =
      getOrElse(accountSegmentInverseKey(ref), none[String])

    override def addAccount(ref: AccountRef, ac: Account): Future[Unit] =
      account(ref).flatMap {
        case None                      => update(ref, ac)
        case Some(a) if ac.rev > a.rev => update(ref, ac)
        case Some(a) =>
          log.warn(
            s"Account ${ac.label} already indexed at higher revision, current revision: ${a.rev}, update revision: ${ac.rev}")
          Future.successful(())
      }

    override def deprecateAccount(ref: AccountRef, rev: Long): Future[Unit] =
      account(ref).flatMap {
        case Some(a) if !a.deprecated && rev > a.rev => update(ref, a.copy(rev = rev, deprecated = true))
        case Some(a) =>
          log.warn(
            s"Trying to deprecate an account '${a.label}' that is already indexed at higher revision, current revision: ${a.rev}, update revision: $rev")
          Future.successful(())
        case _ =>
          log.warn(s"Trying to deprecate account which is not in the cache: uuid:'${ref.id}', rev: $rev")
          Future.failed(
            new RetriableErr(s"Trying to deprecate account which is not in the cache: uuid:'${ref.id}', rev: $rev"))
      }

    private def updateProject(ref: ProjectRef, proj: Project): Future[Unit] = {
      val empty  = LWWRegister(RevisionedValue[Option[Project]](0L, None))
      val value  = RevisionedValue[Option[Project]](proj.rev, Some(proj))
      val update = Update(projectKey(ref), empty, WriteMajority(tm.duration))(_.withValue(value))
      (replicator ? update).flatMap(handleUpdate(s"update project ${proj.label}"))
    }

    private def updateProjectLabelToUuid(ref: ProjectRef,
                                         accountRef: AccountRef,
                                         proj: Project,
                                         accountLabelOpt: Option[String]): Future[Unit] = accountLabelOpt match {
      case Some(accountLabel) =>
        val empty = LWWRegister(RevisionedValue[Option[ProjectRef]](0L, None))
        val value = RevisionedValue[Option[ProjectRef]](proj.rev, Some(ref))
        val update = Update(projectSegmentKey(ProjectLabel(accountLabel, proj.label)),
                            empty,
                            WriteMajority(tm.duration))(_.withValue(value))
        (replicator ? update).flatMap(handleUpdate(s"update project label to uuid mapping for ${proj.label}"))
      case None =>
        log.warn(s"Couldn't find account label for ${accountRef.id} while updating project ${proj.label}")
        Future.failed(
          new RetriableErr(s"Couldn't find account label for ${accountRef.id} while updating project ${proj.label}"))
    }

    private def updateProject(ref: ProjectRef, accountRef: AccountRef, proj: Project): Future[Unit] = {
      for {
        _          <- updateProject(ref, proj)
        _          <- addProjectToAccount(ref, accountRef)
        _          <- addAccountRef(ref, accountRef, proj.rev)
        accountSeg <- accountSegment(accountRef)
        _          <- updateProjectLabelToUuid(ref, accountRef, proj, accountSeg)
      } yield ()
    }

    /**
      * @return true if the project ref is already present so that we can chain the call during an update
      */
    private def addProjectToAccount(ref: ProjectRef, accountRef: AccountRef): Future[Unit] = {
      projects(accountRef).flatMap { projects =>
        if (projects.contains(ref)) Future.successful(())
        else {
          val empty = LWWRegister(RevisionedValue(0L, Set.empty[ProjectRef]))
          val update = Update(accountProjectsKey(accountRef), empty, WriteMajority(tm.duration)) { currentState =>
            val currentRevision = currentState.value.rev
            val currentValue    = currentState.value.value
            currentValue.find(_.id == ref.id) match {
              case Some(_) =>
                currentState
              case None =>
                currentState.withValue(RevisionedValue(currentRevision + 1, currentValue + ref))
            }
          }
          (replicator ? update).flatMap(handleUpdate(s"add project ${ref.id} to account ${accountRef.id}"))
        }
      }
    }

    private def removeProjectFromAccount(ref: ProjectRef, accountRef: AccountRef): Future[Unit] = {
      projects(accountRef).flatMap { projects =>
        if (projects.contains(ref)) {
          val empty = LWWRegister(RevisionedValue(0L, Set.empty[ProjectRef]))
          val update = Update(accountProjectsKey(accountRef), empty, WriteMajority(tm.duration)) { currentState =>
            val currentRevision = currentState.value.rev
            val currentValue    = currentState.value.value
            currentValue.find(_.id == ref.id) match {
              case Some(r) =>
                currentState.withValue(RevisionedValue(currentRevision + 1, currentValue - r))
              case None => currentState
            }
          }
          (replicator ? update).flatMap(handleUpdate(s"remove project ${ref.id} from account ${accountRef.id}"))
        } else Future.successful(())
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

    override def addProject(ref: ProjectRef, accountRef: AccountRef, proj: Project): Future[Unit] =
      project(ref).flatMap {
        case None                        => updateProject(ref, accountRef, proj)
        case Some(p) if proj.rev > p.rev => updateProject(ref, accountRef, proj)
        case Some(p) =>
          log.warn(
            s"Account ${proj.label} already indexed at higher revision, current revision: ${p.rev}, update revision: ${proj.rev}")
          Future.successful(())
      }

    override def deprecateProject(ref: ProjectRef, accountRef: AccountRef, rev: Long): Future[Unit] =
      project(ref).flatMap {
        case Some(p) if !p.deprecated && rev > p.rev =>
          updateProject(ref, p.copy(rev = rev, deprecated = true))
            .flatMap(_ => removeProjectFromAccount(ref, accountRef))
        case Some(p) =>
          log.warn(
            s"Account ${p.label} already indexed at higher revision, current revision: ${p.rev}, update revision: $rev")
          Future.successful(())

        case _ =>
          log.warn(s"Trying to deprecate project which is not in the cache: uuid:'${ref.id}', rev: $rev")
          Future.failed(
            new RetriableErr(s"Trying to deprecate project which is not in the cache: uuid:'${ref.id}', rev: $rev"))
      }

    override def projects(ref: AccountRef): Future[Set[ProjectRef]] =
      getOrElse(accountProjectsKey(ref), Set.empty[ProjectRef])

    override def resolvers(ref: ProjectRef): Future[Set[Resolver]] =
      getOrElse(projectResolversKey(ref), Set.empty[Resolver])

    private def getOrElse[T](f: => LWWRegisterKey[RevisionedValue[T]], default: => T): Future[T] =
      (replicator ? Get(f, ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(f).value.value
        case NotFound(_, _)                       => default
      }

    override def addResolver(
        ref: ProjectRef,
        resolver: Resolver,
    ): Future[Unit] = {

      val empty = LWWRegister(RevisionedValue(0L, Set.empty[Resolver]))

      val update = Update(projectResolversKey(ref), empty, WriteMajority(tm.duration))(updateWithIncrement(_, resolver))
      (replicator ? update).flatMap(handleUpdate(s"add resolver ${resolver.id.show} to project ${ref.id}"))
    }

    override def removeResolver(ref: ProjectRef, id: AbsoluteIri, rev: Long): Future[Unit] = {
      val empty  = LWWRegister(RevisionedValue(0L, Set.empty[Resolver]))
      val update = Update(projectResolversKey(ref), empty, WriteMajority(tm.duration))(removeWithIncrement(_, id, rev))
      (replicator ? update).flatMap(handleUpdate(s"remove resolver ${id.show} from project ${ref.id}"))
    }

    private def updateWithIncrement[A: RevisionedId](currentState: LWWRegister[RevisionedValue[Set[A]]],
                                                     value: A): LWWRegister[RevisionedValue[Set[A]]] = {
      val currentRevision = currentState.value.rev
      val current         = currentState.value.value

      current.find(_.id == value.id) match {
        case Some(r) if r.rev >= value.rev => currentState
        case Some(r) =>
          val updated  = current - r + value
          val newValue = RevisionedValue(currentRevision + 1, updated)
          currentState.withValue(newValue)
        case None =>
          val updated  = current + value
          val newValue = RevisionedValue(currentRevision + 1, updated)
          currentState.withValue(newValue)
      }
    }

    private def removeWithIncrement[A: RevisionedId](currentState: LWWRegister[RevisionedValue[Set[A]]],
                                                     id: AbsoluteIri,
                                                     rev: Long): LWWRegister[RevisionedValue[Set[A]]] = {
      val currentRevision = currentState.value.rev
      val current         = currentState.value.value

      current.find(_.id == id) match {
        case Some(r) if r.rev >= rev => currentState
        case Some(r) =>
          val updated  = current - r
          val newValue = RevisionedValue(currentRevision + 1, updated)
          currentState.withValue(newValue)
        case None => currentState
      }
    }

    override def views(ref: ProjectRef): Future[Set[View]] =
      getOrElse(projectViewsKey(ref), Set.empty[View])

    override def views(label: ProjectLabel): Future[Set[View]] =
      projectRef(label).flatMap {
        case Some(ref) => views(ref)
        case _         => Future(Set.empty)
      }

    override def addView(ref: ProjectRef, view: View): Future[Unit] = {
      val empty  = LWWRegister(RevisionedValue(0L, Set.empty[View]))
      val update = Update(projectViewsKey(ref), empty, WriteMajority(tm.duration))(updateWithIncrement(_, view))
      (replicator ? update).flatMap(handleUpdate(s"add view ${view.id.show} to project ${ref.id}"))
    }

    override def removeView(ref: ProjectRef, id: AbsoluteIri, rev: Long): Future[Unit] = {
      val empty  = LWWRegister(RevisionedValue(0L, Set.empty[View]))
      val update = Update(projectViewsKey(ref), empty, WriteMajority(tm.duration))(removeWithIncrement(_, id, rev))
      (replicator ? update).flatMap(handleUpdate(s"remove resolver ${id.show} from project ${ref.id}"))
    }

    private def handleUpdate(action: String): PartialFunction[Any, Future[Unit]] = {
      case UpdateSuccess(LWWRegisterKey(_), _) =>
        Future.successful(())
      case UpdateTimeout(LWWRegisterKey(_), _) =>
        Future.failed(OperationTimedOut(s"Distributed cache update timed out while performing action '$action'"))
      case ModifyFailure(LWWRegisterKey(_), msg, cause, _) =>
        log.error(s"Failed to modify the current value while performing action: '$action' with error message: '$msg'",
                  cause)
        Future.failed(cause)
      case StoreFailure(LWWRegisterKey(_), _) =>
        log.error(s"Failed to replicate the update for: '$action'")
        Future.failed(new RetriableErr(s"Failed to replicate the update for: '$action'"))

    }

    private def none[A]: Option[A] = None
  }

  /**
    * Constructs a ''DistributedCache'' instance in a ''Task'' effect type.
    *
    * @param as the underlying actor system
    * @param tm timeout used for the lookup operations
    */
  def task()(implicit as: ActorSystem, tm: Timeout): DistributedCache[Task] =
    new DistributedCache[Task] {

      private val underlying = future()

      override def views(label: ProjectLabel): Task[Set[View]] =
        Task.deferFuture(underlying.views(label))

      override def projectRef(label: ProjectLabel): Task[Option[ProjectRef]] =
        Task.deferFuture(underlying.projectRef(label))

      override def project(label: ProjectLabel): Task[Option[Project]] =
        Task.deferFuture(underlying.project(label))

      override def account(ref: AccountRef): Task[Option[Account]] =
        Task.deferFuture(underlying.account(ref))

      override def accountRef(ref: ProjectRef): Task[Option[AccountRef]] =
        Task.deferFuture(underlying.accountRef(ref))

      override def addAccount(ref: AccountRef, account: Account): Task[Unit] =
        Task.deferFuture(underlying.addAccount(ref, account))

      override def deprecateAccount(ref: AccountRef, rev: Long): Task[Unit] =
        Task.deferFuture(underlying.deprecateAccount(ref, rev))

      override def project(ref: ProjectRef): Task[Option[Project]] =
        Task.deferFuture(underlying.project(ref))

      override def addProject(ref: ProjectRef, accountRef: AccountRef, project: Project): Task[Unit] =
        Task.deferFuture(underlying.addProject(ref, accountRef, project))

      override def deprecateProject(ref: ProjectRef, accountRef: AccountRef, rev: Long): Task[Unit] =
        Task.deferFuture(underlying.deprecateProject(ref, accountRef, rev))

      override def projects(ref: AccountRef): Task[Set[ProjectRef]] =
        Task.deferFuture(underlying.projects(ref))

      override def resolvers(ref: ProjectRef): Task[Set[Resolver]] =
        Task.deferFuture(underlying.resolvers(ref))

      override def addResolver(ref: ProjectRef, resolver: Resolver): Task[Unit] =
        Task.deferFuture(underlying.addResolver(ref, resolver))

      override def removeResolver(ref: ProjectRef, id: AbsoluteIri, rev: Long): Task[Unit] =
        Task.deferFuture(underlying.removeResolver(ref, id, rev))

      override def views(ref: ProjectRef): Task[Set[View]] =
        Task.deferFuture(underlying.views(ref))

      override def addView(ref: ProjectRef, view: View): Task[Unit] =
        Task.deferFuture(underlying.addView(ref, view))

      override def removeView(ref: ProjectRef, id: AbsoluteIri, rev: Long): Task[Unit] =
        Task.deferFuture(underlying.removeView(ref, id, rev))
    }
}
