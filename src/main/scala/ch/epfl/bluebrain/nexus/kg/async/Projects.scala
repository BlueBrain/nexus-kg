package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.ActorSystem
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{DistributedData, LWWRegisterKey}
import akka.pattern.ask
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectRef}
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
  def revolvers(ref: ProjectRef): F[Set[Resolver]]

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

    override def revolvers(ref: ProjectRef): Future[Set[Resolver]] =
      (replicator ? Get(resolverKey(ref), ReadLocal, None)).map {
        case g @ GetSuccess(LWWRegisterKey(_), _) => g.get(resolverKey(ref)).value.value
        case NotFound(_, _)                       => Set.empty
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

      override def revolvers(ref: ProjectRef): Task[Set[Resolver]] =
        Task.deferFuture(underlying.revolvers(ref))

      override def views(ref: ProjectRef): Task[Set[View]] =
        Task.deferFuture(underlying.views(ref))
    }
}
