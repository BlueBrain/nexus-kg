package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Functor
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.resources.Ref._
import ch.epfl.bluebrain.nexus.kg.resources._

/**
  * Simplest implementation that handles the resolution process of references to resources
  * within a given project.
  *
  * @param project the resolution scope
  * @tparam F      the resolution effect type
  */
class InProjectResolution[F[_]: Functor: Repo](project: ProjectRef) extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] = ref match {
    case Latest(value)        => Resources.get(Id(project, value)).value
    case Revision(value, rev) => Resources.get(Id(project, value), rev).value
    case Tag(value, tag)      => Resources.get(Id(project, value), tag).value
  }

  override def resolveAll(ref: Ref): F[List[Resource]] =
    resolve(ref).map(_.toList)
}

object InProjectResolution {

  /**
    * Constructs an [[InProjectResolution]] instance.
    */
  def apply[F[_]: Functor: Repo](project: ProjectRef): InProjectResolution[F] =
    new InProjectResolution(project)
}
