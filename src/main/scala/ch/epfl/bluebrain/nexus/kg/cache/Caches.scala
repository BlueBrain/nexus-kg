package ch.epfl.bluebrain.nexus.kg.cache

/**
  * Aggregator of the caches used in the service
  *
  * @param project  the project cache
  * @param view     the view cache
  * @param resolver the resolver cache
  * @param storage  the storage cache
  * @tparam F the effect type
  */
final class Caches[F[_]](val project: ProjectCache[F],
                         val view: ViewCache[F],
                         val resolver: ResolverCache[F],
                         val storage: StorageCache[F])
object Caches {

  /**
    * Constructor for [[Caches]]
    *
    * @param project  the project cache
    * @param view     the view cache
    * @param resolver the resolver cache
    * @tparam F the effect type
    */
  final def apply[F[_]](project: ProjectCache[F],
                        view: ViewCache[F],
                        resolver: ResolverCache[F],
                        storage: StorageCache[F]): Caches[F] =
    new Caches(project, view, resolver, storage)

  // $COVERAGE-OFF$
  final implicit def viewCache[F[_]](implicit caches: Caches[F]): ViewCache[F]       = caches.view
  final implicit def storageCache[F[_]](implicit caches: Caches[F]): StorageCache[F] = caches.storage
  final implicit def projectCache[F[_]](implicit caches: Caches[F]): ProjectCache[F] = caches.project
  // $COVERAGE-ON$
}
