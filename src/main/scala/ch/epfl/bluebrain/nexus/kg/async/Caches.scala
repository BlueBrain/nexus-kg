package ch.epfl.bluebrain.nexus.kg.async

/**
  * Aggregator of the caches used in the service
  *
  * @param project      the project cache
  * @param view         the view cache
  * @param resolver     the resolver cache
  * @tparam F the effet type
  */
final case class Caches[F[_]](project: ProjectCache[F], view: ViewCache[F], resolver: ResolverCache[F])
