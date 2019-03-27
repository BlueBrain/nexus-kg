package ch.epfl.bluebrain.nexus.kg.resolve

import cats.data.EitherT
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.IllegalContextValue
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{blank, IriNode}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Iri, RootedGraph}
import io.circe.Json
import io.circe.syntax._

class Materializer[F[_]: Effect](repo: Repo[F], resolution: ProjectResolution[F])(implicit config: AppConfig) {

  private val emptyJson = Json.obj()

  private def flattenCtx(refs: List[Ref], contextValue: Json)(implicit project: Project): EitherT[F, Rejection, Json] =
    (contextValue.asString, contextValue.asArray, contextValue.asObject) match {
      case (Some(str), _, _) =>
        val nextRef = Iri.absolute(str).toOption.map(Ref.apply)
        for {
          next  <- EitherT.fromOption[F](nextRef, IllegalContextValue(refs))
          res   <- resolveOrNotFound(next)
          value <- flattenCtx(next :: refs, res.value.contextValue)
        } yield value
      case (_, Some(arr), _) =>
        val jsons = arr.traverse(j => flattenCtx(refs, j).value).map(_.sequence)

        EitherT(jsons).map(_.foldLeft(Json.obj())(_ deepMerge _))
      case (_, _, Some(_)) => EitherT.rightT[F, Rejection](contextValue)
      case (_, _, _)       => EitherT.leftT[F, Json](IllegalContextValue(refs): Rejection)
    }

  private def resolveOrNotFound(ref: Ref)(implicit project: Project): RejOrResource[F] =
    EitherT.fromOptionF(resolution(project.ref)(repo).resolve(ref), notFound(ref))

  /**
    * Resolves the context URIs using the [[ProjectResolution]] and once resolved, it replaces the URIs for the actual payload.
    *
    * @param source  the payload to resolve and flatten
    * @param project the project where the payload belongs to
    * @return Left(rejection) when failed and Right(value) when passed, wrapped in the effect type ''F''
    */
  def apply(source: Json)(implicit project: Project): EitherT[F, Rejection, Value] =
    flattenCtx(Nil, source.contextValue)
      .map {
        case `emptyJson` => Json.obj("@base" -> project.base.asString.asJson, "@vocab" -> project.vocab.asString.asJson)
        case flattened   => flattened
      }
      .flatMap { ctx =>
        val value = source.deepMerge(Json.obj("@context" -> ctx)).asGraph(blank).map(Value(source, ctx, _))
        EitherT.fromEither[F](value).leftSemiflatMap(e => Rejection.fromMarshallingErr[F](e))
      }

  /**
    * Attempts to find a resource with the provided ref using the [[ProjectResolution]]. Once found, it attempts to resolve the context URIs
    *
    * @param ref     the reference to a resource in the platform
    * @param project the current project
    * @return Left(rejection) when failed and Right(value) when passed, wrapped in the effect type ''F''
    */
  def apply(ref: Ref)(implicit project: Project): RejOrResourceV[F] =
    for {
      resource <- EitherT.fromOptionF(resolution(project.ref)(repo).resolve(ref), notFound(ref))
      value    <- apply(resource.value)
    } yield resource.map(_ => value.copy(graph = value.graph.removeMetadata))

  /**
    * Materializes a resource flattening its context and producing a raw graph. While flattening the context references
    * are transitively resolved. If the provided context and resulting graph are empty, the parent project's base and
    * vocab settings are injected as the context in order to recompute the graph from the original JSON source.
    *
    * @param resource the resource to materialize
    */
  def apply(resource: Resource)(implicit project: Project): RejOrResourceV[F] =
    apply(resource.value).map {
      case Value(json, flattened, graph) => resource.map(_ => Value(json, flattened, graph.removeMetadata))
    }

  /**
    * Materializes a resource flattening its context and producing a raw graph with the resource metadata. While flattening the context references
    * are transitively resolved. If the provided context and resulting graph are empty, the parent project's base and
    * vocab settings are injected as the context in order to recompute the graph from the original JSON source.
    *
    * @param resource the resource to materialize
    */
  def withMeta(resource: Resource, selfAsIri: Boolean = false)(implicit project: Project): RejOrResourceV[F] =
    apply(resource).map { resourceV =>
      val graph =
        RootedGraph(resourceV.value.graph.rootNode, resourceV.value.graph.triples ++ resourceV.metadata(selfAsIri))
      resourceV.map(_.copy(graph = graph))
    }

  /**
    * Transitively imports resources referenced by the primary node of the resource through ''owl:imports'' if the
    * resource has type ''owl:Ontology''.
    *
    * @param resId the resource id for which imports are looked up
    * @param graph the resource graph for which imports are looked up
    */
  def imports(resId: ResId, graph: Graph)(implicit project: Project): EitherT[F, Rejection, Set[ResourceV]] = {

    def importsValues(id: AbsoluteIri, g: Graph): Set[Ref] =
      g.objects(IriNode(id), owl.imports).unorderedFoldMap {
        case IriNode(iri) => Set(iri.ref)
        case _            => Set.empty
      }

    def lookup(current: Map[Ref, ResourceV], remaining: List[Ref]): EitherT[F, Rejection, Set[ResourceV]] = {
      def load(ref: Ref): EitherT[F, Rejection, (Ref, ResourceV)] =
        current
          .find(_._1 == ref)
          .map(tuple => EitherT.rightT[F, Rejection](tuple))
          .getOrElse(apply(ref).map(ref -> _))

      if (remaining.isEmpty) EitherT.rightT[F, Rejection](current.values.toSet)
      else {
        val batch: EitherT[F, Rejection, List[(Ref, ResourceV)]] =
          remaining.traverse(load)

        batch.flatMap { list =>
          val nextRemaining: List[Ref] = list.flatMap {
            case (ref, res) => importsValues(ref.iri, res.value.graph).toList
          }
          val nextCurrent: Map[Ref, ResourceV] = current ++ list.toMap
          lookup(nextCurrent, nextRemaining)
        }
      }
    }

    lookup(Map.empty, importsValues(resId.value, graph).toList)
  }

}
