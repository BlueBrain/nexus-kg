package ch.epfl.bluebrain.nexus.kg.async

/**
  * Aggregator of the caches used in the service
  *
  * @param project      the project cache
  * @param view         the view cache
  * @param resolver     the resolver cache
  * @tparam F the effect type
  */
final class Caches[F[_]](val project: ProjectCache[F], val view: ViewCache[F], val resolver: ResolverCache[F])
object Caches {

  /**
    * Constructor for [[Caches]]
    *
    * @param project  the project cache
    * @param view     the view cache
    * @param resolver the resolver cache
    * @tparam F the effect type
    */
  final def apply[F[_]](project: ProjectCache[F], view: ViewCache[F], resolver: ResolverCache[F]): Caches[F] =
    new Caches(project, view, resolver)
}
