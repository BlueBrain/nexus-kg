package ch.epfl.bluebrain.nexus.kg.resources

import cats.data.EitherT
import cats.{Functor, Monad}
import ch.epfl.bluebrain.nexus.kg.resolve.Resolution
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json

/**
  * Resource operations.
  */
object Resources {

  /**
    * Creates a new resource.
    *
    * @param id              the id of the resource
    * @param schema          a schema reference that constrains the resource
    * @param additionalTypes a collection of additional (asserted or inferred) types of the resource
    * @param source          the source representation in json-ld format
    * @return either a rejection or the newly created resource in the F context
    */
  def create[F[_]: Resolution: Monad](
      id: ResId,
      schema: Ref,
      additionalTypes: Set[AbsoluteIri],
      source: Json
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] =
    // format: off
    for {
      value                       <- materialize[F](id, source)
      graph                        = value.graph
      resolvedSchema              <- EitherT.fromOptionF(schema.resolve, NotFound(schema))
      materializedSchema          <- materialize(resolvedSchema)
      importedResources           <- imports(materializedSchema)
      (schemaImports, dataImports) = partition(importedResources)
      _                           <- validate(materializedSchema, schemaImports, dataImports, graph)
      types                        = joinTypes(id, graph, additionalTypes)
      created                     <- repo.create(id, schema, types, source, identity)
    } yield created
    // format: on

  def joinTypes(id: ResId, graph: Graph, additional: Set[AbsoluteIri]): Set[AbsoluteIri] = ???

  def partition(set: Set[ResourceV]): (Set[ResourceV], Set[ResourceV]) = ???

  def materialize[F[_]: Resolution](id: ResId, source: Json): EitherT[F, Rejection, ResourceF.Value] = ???

  def materialize[F[_]: Functor: Resolution](resource: Resource): EitherT[F, Rejection, ResourceV] =
    for {
      value <- materialize(resource.id, resource.value)
    } yield resource.map(_ => value)

  def imports[F[_]](resource: ResourceV): EitherT[F, Rejection, Set[ResourceV]] = ???

  def validate[F[_]](schema: ResourceV,
                     schemaImports: Set[ResourceV],
                     dataImports: Set[ResourceV],
                     data: Graph): EitherT[F, Rejection, Unit] = ???

}
