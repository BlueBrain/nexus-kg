package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.resources.Ref._
import ch.epfl.bluebrain.nexus.kg.resources._

/**
  * Simplest implementation that handles the resolution process of references to resources
  * within a given project.
  *
  * @param project the resolution scope
  * @param resources the resources operations
  * @tparam F      the resolution effect type
  */
class InProjectResolution[F[_]: Monad](project: ProjectRef, resources: Resources[F]) extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] = ref match {
    case Latest(value)        => resources.fetch(Id(project, value), None).value
    case Revision(value, rev) => resources.fetch(Id(project, value), rev, None).value
    case Tag(value, tag)      => resources.fetch(Id(project, value), tag, None).value
  }

  override def resolveAll(ref: Ref): F[List[Resource]] =
    resolve(ref).map(_.toList)
}

object InProjectResolution {

  /**
    * Constructs an [[InProjectResolution]] instance.
    */
  def apply[F[_]: Monad](project: ProjectRef, resources: Resources[F]): InProjectResolution[F] =
    new InProjectResolution(project, resources)
}
