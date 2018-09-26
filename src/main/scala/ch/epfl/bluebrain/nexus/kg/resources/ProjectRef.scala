package ch.epfl.bluebrain.nexus.kg.resources

import cats.{Monad, Show}
import cats.data.OptionT
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache

/**
  * A stable project reference.
  *
  * @param id the underlying stable identifier for a project
  */
final case class ProjectRef(id: String) extends AnyVal { self =>

  /**
    * Attempt to fetch the [[ProjectLabel]] from the actual [[ProjectRef]]
    *
    * @param cache where to find the mapping between [[ProjectRef]] and [[ProjectLabel]]
    * @tparam F the effect type
    * @return an option of [[ProjectLabel]] wrapped on ''F[_]''
    */
  def toLabel[F[_]: Monad](cache: DistributedCache[F]): F[Option[ProjectLabel]] =
    (for {
      accountRef <- OptionT(cache.accountRef(self))
      account    <- OptionT(cache.account(accountRef))
      project    <- OptionT(cache.project(self))
    } yield ProjectLabel(account.label, project.label)).value
}

object ProjectRef {

  final implicit val projectRefShow: Show[ProjectRef] = Show.fromToString
}
