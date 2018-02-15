package ch.epfl.bluebrain.nexus.kg.service.contexts

import cats.instances.future._
import cats.instances.try_._
import cats.instances.vector._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.show._
import cats.{MonadError, Traverse}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.service.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.service.contexts.Resolver.IllegalImportsViolation
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceRejection
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceRejection.ShapeConstraintViolations
import io.circe.syntax._
import io.circe.{Json, JsonObject}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[contexts] class Resolver[F[_]](implicit F: MonadError[F, Throwable], f: ContextId => F[Option[Json]]) {

  private def apply(json: Json, base: String): F[Json] = {

    def resolveValue(context: Json): F[Json] =
      (context.asString, context.asArray, context.asObject) match {
        case (Some(str), _, _) =>
          val start = s"$base/${Resolver.contextPath}/"
          if (!str.startsWith(start))
            F.raiseError(importsViolation(s"Referenced context '$str' is not managed in this platform"))
          else {
            ContextId(str.substring(start.length)) match {
              case Some(id) =>
                f(id).flatMap {
                  case Some(ctx) => ctx.hcursor.get[Json]("@context").map(resolveValue).getOrElse(F.pure(Json.obj()))
                  case None      => F.raiseError(importsViolation(s"Referenced context '$str' does not exist"))
                }
              case None => F.raiseError(importsViolation(s"Referenced context '$str' is not managed in this platform"))
            }
          }
        case (_, Some(arr), _) =>
          Traverse[Vector]
            .sequence(arr.map(v => resolveValue(v)))
            .map(values =>
              values.foldLeft(Json.obj()) { (acc, e) =>
                acc deepMerge e
            })
        case (_, _, Some(_)) => F.pure(context)
        case (_, _, _)       => F.raiseError(CommandRejected(ShapeConstraintViolations(List("Illegal context format"))))
      }

    def inner(jsonObj: JsonObject): F[JsonObject] =
      Traverse[Vector]
        .sequence(jsonObj.toVector.map {
          case ("@context", v) => resolveValue(v).map("@context" -> _)
          case (k, v)          => apply(v, base).map(k           -> _)
        })
        .map(JsonObject.fromIterable)

    json.arrayOrObject(F.pure(json),
                       arr => Traverse[Vector].sequence(arr.map(apply(_, base))).map(Json.fromValues),
                       obj => inner(obj).map(_.asJson))
  }

  private def importsViolation(message: String) =
    CommandRejected(IllegalImportsViolation(Set(message)))
}

object Resolver {

  private[contexts] val contextPath = "contexts"

  private[contexts] class ResolverLocalSyntax[F[_]](json: Json)(implicit F: MonadError[F, Throwable])
      extends Resources {

    def resolvedFor(prefix: Path): F[Json] = {
      implicit val f = (id: ContextId) =>
        F.pure(Try(jsonContentOf((prefix ++ Path(contextPath) ++ Path(s"${id.show}.json")).toString())).toOption)

      val resolver = new Resolver[F]()
      resolver(json, prefix.toString())
    }
  }

  private[contexts] class ResolverUriSyntax[F[_]](json: Json)(implicit F: MonadError[F, Throwable],
                                                              contexts: Contexts[F],
                                                              httpConfig: HttpConfig) {
    private implicit val f = (id: ContextId) => contexts.fetchValue(id)
    private val resolver   = new Resolver[F]()
    def resolved: F[Json]  = resolver(json, httpConfig.publicUri.toString())
  }

  object try_ {
    implicit class ResolverLocalSyntaxTry(json: Json) extends ResolverLocalSyntax[Try](json)
    implicit class ResolverUriSyntaxTry(json: Json)(implicit contexts: Contexts[Try], httpConfig: HttpConfig)
        extends ResolverUriSyntax[Try](json)
  }

  object future {
    implicit class ResolverUriSyntaxFuture(json: Json)(implicit ec: ExecutionContext,
                                                       contexts: Contexts[Future],
                                                       httpConfig: HttpConfig)
        extends ResolverUriSyntax[Future](json)
  }

  /**
    * Signals the failure to perform a context modification due to payload illegal imports.
    *
    * @param imports the collections of imports that are not accepted
    */
  final case class IllegalImportsViolation(imports: Set[String]) extends ResourceRejection
}
