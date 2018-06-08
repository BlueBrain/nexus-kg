package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.kg.resolve.Resolution

object syntax {

  final implicit class RefSyntax[F[_]](ref: Ref)(implicit R: Resolution[F]) {
    def resolve: F[Option[Resource]]  = R.resolve(ref)
    def resolveAll: F[List[Resource]] = R.resolveAll(ref)
  }

}
