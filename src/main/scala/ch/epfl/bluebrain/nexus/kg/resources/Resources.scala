package ch.epfl.bluebrain.nexus.kg.resources

import cats.Monad
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.kg.resolve.Resolution
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{IllegalContextValue, NotFound, UnableToSelectResourceId}
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.nexus._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Iri}
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
  def create[F[_]: Monad: Resolution](
      id: ResId,
      schema: Ref,
      additionalTypes: Set[AbsoluteIri],
      source: Json
  )(implicit repo: Repo[F], identity: Identity): EitherT[F, Rejection, Resource] =
    // format: off
    for {
      value                       <- materialize[F](id, source)
      graph                        = value.graph
      resolvedSchema              <- schema.resolveOr(NotFound)
      materializedSchema          <- materialize(resolvedSchema)
      importedResources           <- imports(materializedSchema)
      (schemaImports, dataImports) = partition(importedResources)
      _                           <- validate(materializedSchema, schemaImports, dataImports, graph)
      types                        = joinTypes(graph, additionalTypes)
      created                     <- repo.create(id, schema, types, source, identity)
    } yield created
    // format: on

  /**
    * Extracts the types of the graph primary node and appends them to the collection of additional types.
    *
    * @param graph      a resource graph
    * @param additional the additional collection of types
    */
  def joinTypes(graph: Graph, additional: Set[AbsoluteIri]): Set[AbsoluteIri] =
    graph.primaryTypes.map(_.value) ++ additional

  /**
    * Partitions the collection of resources based on their types (schemas and others).
    *
    * @param set the collection of resources to partition
    * @return (schemas, others)
    */
  def partition(set: Set[ResourceV]): (Set[ResourceV], Set[ResourceV]) =
    set.partition(_.isSchema)

  /**
    * Materializes a json entity into a ResourceF.Value, flattening its context and producing a raw graph. While
    * flattening the context references are transitively resolved.
    *
    * @param id     the primary id of the entity
    * @param source the source representation of the entity
    */
  def materialize[F[_]: Monad: Resolution](id: ResId, source: Json): EitherT[F, Rejection, ResourceF.Value] = {
    def contextValueOf(json: Json): Json =
      json.hcursor.downField("@context").focus.getOrElse(Json.obj())

    def flattenValue(ref: Ref, contextValue: Json): EitherT[F, Rejection, Json] =
      (contextValue.asString, contextValue.asArray, contextValue.asObject) match {
        case (Some(str), _, _) =>
          val nextRef = Iri.absolute(str).toOption.map(Ref.apply)
          for {
            next  <- EitherT.fromOption[F](nextRef, IllegalContextValue(ref))
            res   <- next.resolveOr(NotFound)
            value <- flattenValue(next, contextValueOf(res.value))
          } yield value
        case (_, Some(arr), _) =>
          import cats.implicits._
          val jsons = arr
            .traverse(j => flattenValue(ref, j).value)
            .map(_.sequence)
          EitherT(jsons).map(_.foldLeft(Json.obj())(_ deepMerge _))
        case (_, _, Some(_)) => EitherT.rightT(contextValue)
        case (_, _, _)       => EitherT.leftT(IllegalContextValue(ref))
      }

    def graphFor(flattenCtx: Json): EitherT[F, Rejection, Graph] = {
      val original = (source deepMerge Json.obj("@context" -> flattenCtx)).asGraph
      val withId =
        if (original.subjects().contains(id.value)) Some(original)
        else
          original.primaryBNode.map { bnode =>
            original.replaceNode(bnode, id.value)
          }
      EitherT.fromOption[F](withId, UnableToSelectResourceId(id.ref))
    }

    for {
      ctx <- flattenValue(id.ref, contextValueOf(source))
      g   <- graphFor(ctx)
    } yield Value(source, ctx, g)
  }

  /**
    * Materializes a resource flattening its context and producing a raw graph. While flattening the context references
    * are transitively resolved.
    *
    * @param resource the resource to materialize
    */
  def materialize[F[_]: Monad: Resolution](resource: Resource): EitherT[F, Rejection, ResourceV] =
    for {
      value <- materialize[F](resource.id, resource.value)
    } yield resource.map(_ => value)

  /**
    * Materializes a resource flattening its context and producing a graph that contains the additional type information
    * and the system generated metadata. While flattening the context references are transitively resolved.
    *
    * @param resource the resource to materialize
    */
  def materializeWithMeta[F[_]: Monad: Resolution](resource: Resource): EitherT[F, Rejection, ResourceV] =
    for {
      resourceV <- materialize[F](resource)
      graph = resourceV.value.graph
      value = resourceV.value.copy(graph = resourceV.metadata(_.iri) ++ resourceV.typeGraph)
    } yield resourceV.map(_ => value)

  def imports[F[_]](resource: ResourceV): EitherT[F, Rejection, Set[ResourceV]] = ???

  def validate[F[_]](schema: ResourceV,
                     schemaImports: Set[ResourceV],
                     dataImports: Set[ResourceV],
                     data: Graph): EitherT[F, Rejection, Unit] = ???

}
