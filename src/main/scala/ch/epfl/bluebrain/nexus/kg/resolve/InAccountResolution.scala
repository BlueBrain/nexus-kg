package ch.epfl.bluebrain.nexus.kg.resolve

import cats.Monad
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.traverse._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.resources._

/**
  * Resolution implementation that looks into every project belonging to a given account, in no particular order.
  *
  * TODO: ACLs
  */
class InAccountResolution[F[_]](accountRef: AccountRef, resources: Resources[F], cache: DistributedCache[F])(
    implicit F: Monad[F])
    extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] =
    cache.projects(accountRef).flatMap {
      _.foldLeft[F[Option[Resource]]](F.pure(None)) {
        case (acc, proj) =>
          acc.flatMap {
            case v @ Some(_) => F.pure(v)
            case None        => InProjectResolution[F](proj, resources).resolve(ref)
          }
      }
    }

  override def resolveAll(ref: Ref): F[List[Resource]] =
    cache.projects(accountRef).flatMap { projs =>
      val res = projs.toList.map(proj => InProjectResolution[F](proj, resources))
      val all = res.map(_.resolveAll(ref))
      all.flatSequence
    }
}

object InAccountResolution {

  /**
    * Constructs an [[InAccountResolution]] instance.
    *
    * @param accountRef the resolution scope
    * @param resources  the resources operations
    * @param cache      the distributed cache
    * @tparam F the resolution effect type
    */
  def apply[F[_]: Monad](accountRef: AccountRef,
                         resources: Resources[F],
                         cache: DistributedCache[F]): InAccountResolution[F] =
    new InAccountResolution(accountRef, resources, cache)
}
