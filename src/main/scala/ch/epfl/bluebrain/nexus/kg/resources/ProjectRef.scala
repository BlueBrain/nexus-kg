package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.{Monad, Show}
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache

/**
  * A stable project reference.
  *
  * @param id the underlying stable identifier for a project
  */
final case class ProjectRef(id: UUID) {

  /**
    * Attempt to fetch the [[ProjectLabel]] from the actual [[ProjectRef]]
    *
    * @param cache where to find the mapping between [[ProjectRef]] and [[ProjectLabel]]
    * @tparam F the effect type
    * @return an option of [[ProjectLabel]] wrapped on ''F[_]''
    */
  def toLabel[F[_]: Monad](cache: DistributedCache[F]): F[Option[ProjectLabel]] =
    cache.project(this).map(_.map(project => ProjectLabel(project.organization, project.label)))
}

object ProjectRef {

  final implicit val projectRefShow: Show[ProjectRef] = Show.show(_.id.toString)
}
