package ch.epfl.bluebrain.nexus.kg.resolve
import java.time.Clock

import cats.Monad
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json

/**
  * Implementation that handles resolution of static resources
  * @param resources  mapping between the URI of the resource and the resource
  * @tparam F         the resolution effect type
  */
class StaticResolution[F[_]](resources: Map[AbsoluteIri, Resource])(implicit F: Monad[F]) extends Resolution[F] {

  override def resolve(ref: Ref): F[Option[Resource]] = F.pure(resources.get(ref.iri))

  override def resolveAll(ref: Ref): F[List[Resource]] =
    resolve(ref).map(_.toList)
}

object StaticResolution {

  /**
    * Constructs a [[StaticResolution]] from mapping between URI and JSON content of the resource
    *
    * @param resources mapping between the URI of the resource and the JSON content
    */
  final def apply[F[_]: Monad](resources: Map[AbsoluteIri, Json])(implicit clock: Clock): StaticResolution[F] =
    new StaticResolution[F](resources.map {
      case (iri, json) => (iri, ResourceF.simpleF(Id(ProjectRef("static"), iri), json))
    })
}
