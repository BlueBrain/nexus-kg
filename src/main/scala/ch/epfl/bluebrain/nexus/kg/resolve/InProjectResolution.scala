package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Applicative
import cats.implicits._
import ch.epfl.bluebrain.nexus.kg.resources._

/**
  * Simplest implementation that handles the resolution process of references to resources
  * within a given project.
  *
  * @param project the resolution scope
  * @param resources the resources operations
  * @tparam F      the resolution effect type
  */
class InProjectResolution[F[_]: Applicative](project: ProjectRef, resources: Resources[F]) extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] = ref match {
    case Ref.Latest(value)        => resources.fetch(Id(project, value)).value.map(_.toOption)
    case Ref.Revision(value, rev) => resources.fetch(Id(project, value), rev).value.map(_.toOption)
    case Ref.Tag(value, tag)      => resources.fetch(Id(project, value), tag).value.map(_.toOption)
  }
}

object InProjectResolution {

  /**
    * Constructs an [[InProjectResolution]] instance.
    */
  def apply[F[_]: Applicative](project: ProjectRef, resources: Resources[F]): InProjectResolution[F] =
    new InProjectResolution(project, resources)
}
