package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.headers.{`WWW-Authenticate`, HttpChallenges, Location}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.iam.client.IamClientError
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError._
import ch.epfl.bluebrain.nexus.kg.async.{Caches, ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.routes.AppInfoRoutes.HealthStatusGroup
import ch.epfl.bluebrain.nexus.kg.routes.HealthStatus._
import ch.epfl.bluebrain.nexus.service.http.RejectionHandling
import ch.epfl.bluebrain.nexus.service.http.directives.PrefixDirectives.uriPrefix
import ch.epfl.bluebrain.nexus.service.http.directives.StatusFrom
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.{cors, corsRejectionHandler}
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import io.circe.parser.parse
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.util.control.NonFatal

object Routes {

  private[this] val logger = Logger[this.type]

  /**
    * @return an ExceptionHandler that ensures a descriptive message is returned to the caller
    */
  final val exceptionHandler: ExceptionHandler = {
    def completeGeneric(): Route = {
      val error: KgError = InternalError("The system experienced an unexpected error, please try again later.")
      complete(KgError.kgErrorStatusFrom(error) -> error)
    }

    ExceptionHandler {
      case _: IamClientError.Unauthorized =>
        // suppress errors for authentication failures
        val status         = KgError.kgErrorStatusFrom(AuthenticationFailed)
        val header         = `WWW-Authenticate`(HttpChallenges.oAuth2("*"))
        val error: KgError = AuthenticationFailed
        complete((status, List(header), error))
      case _: IamClientError.Forbidden =>
        // suppress errors for authorization failures
        complete(KgError.kgErrorStatusFrom(AuthorizationFailed) -> (AuthorizationFailed: KgError))
      case err: NotFound =>
        // suppress errors for not found
        complete(KgError.kgErrorStatusFrom(err) -> (err: KgError))
      case AuthenticationFailed =>
        // suppress errors for authentication failures
        val status         = KgError.kgErrorStatusFrom(AuthenticationFailed)
        val header         = `WWW-Authenticate`(HttpChallenges.oAuth2("*"))
        val error: KgError = AuthenticationFailed
        complete((status, List(header), error))
      case AuthorizationFailed =>
        // suppress errors for authorization failures
        complete(KgError.kgErrorStatusFrom(AuthorizationFailed) -> (AuthorizationFailed: KgError))
      case err: ProjectNotFound =>
        // suppress error
        complete(KgError.kgErrorStatusFrom(err) -> (err: KgError))
      case err: ProjectIsDeprecated =>
        // suppress error
        complete(KgError.kgErrorStatusFrom(err) -> (err: KgError))
      case ElasticClientError(status, body) =>
        // TODO: discriminate between listing failures and view querying
        parse(body) match {
          case Right(json) => complete(status -> json)
          case Left(_)     => complete(status -> body)
        }
      case f: ElasticFailure =>
        logger.error(s"Received unexpected response from ES: '${f.message}' with body: '${f.body}'")
        completeGeneric()
      case err: KgError =>
        logger.error(s"Exception caught during routes processing", err)
        completeGeneric()
      case NonFatal(err) =>
        logger.error("Exception caught during routes processing", err)
        completeGeneric()
    }
  }

  /**
    * @return a complete RejectionHandler for all library and code rejections
    */
  final val rejectionHandler: RejectionHandler = {
    val statusFrom: StatusFrom[Rejection] = implicitly[StatusFrom[Rejection]]
    val custom = RejectionHandling.apply[Rejection]({ r: Rejection =>
      logger.debug(s"Handling rejection '$rejection'")
      statusFrom(r) -> r
    })
    corsRejectionHandler withFallback custom withFallback RejectionHandling.notFound withFallback RejectionHandler.default
  }

  /**
    * Wraps the provided route with CORS, rejection and exception handling.
    *
    * @param route the route to wrap
    */
  final def wrap(route: Route)(implicit hc: HttpConfig): Route = {
    val corsSettings = CorsSettings.defaultSettings
      .withAllowedMethods(List(GET, PUT, POST, DELETE, OPTIONS, HEAD))
      .withExposedHeaders(List(Location.name))
    cors(corsSettings) {
      handleExceptions(exceptionHandler) {
        handleRejections(rejectionHandler) {
          uriPrefix(hc.publicUri) {
            route
          }
        }
      }
    }
  }

  /**
    * Generates the routes for all the platform resources
    *
    * @param resources the resources operations
    */
  def apply(resources: Resources[Task])(
      implicit as: ActorSystem,
      clients: Clients[Task],
      cache: Caches[Task],
      store: FileStore[Task, AkkaIn, AkkaOut],
      config: AppConfig,
  ): Route = {
    import clients._
    implicit val um: FromEntityUnmarshaller[String] =
      PredefinedFromEntityUnmarshallers.stringUnmarshaller
        .forContentTypes(RdfMediaTypes.`application/sparql-query`, MediaTypes.`text/plain`)

    implicit val projectCache: ProjectCache[Task] = cache.project
    implicit val viewCache: ViewCache[Task]       = cache.view

    val healthStatusGroup = HealthStatusGroup(
      new CassandraHealthStatus(),
      new ClusterHealthStatus(Cluster(as)),
      new IamHealthStatus(clients.iamClient),
      new AdminHealthStatus(clients.adminClient),
      new ElasticSearchHealthStatus(clients.elastic),
      new SparqlHealthStatus(clients.sparql)
    )
    val appInfoRoutes = AppInfoRoutes(config.description, healthStatusGroup).routes

    wrap(
      pathPrefix(config.http.prefix / Segment) { resourceSegment =>
        extractToken {
          implicit optToken =>
            project.apply {
              implicit project =>
                (extractCallerAcls & extractCaller) { (acl, c) =>
                  resourceSegment match {
                    case "resolvers" => new ResolverRoutes(resources, acl, c).routes
                    case "views"     => new ViewRoutes(resources, acl, c).routes
                    case "schemas"   => new SchemaRoutes(resources, acl, c).routes
                    case "files"     => new FileRoutes(resources, acl, c).routes
                    case "resources" => new ResourceRoutes(resources, acl, c).routes
                    case _           => reject()
                  }
                }
            }
        }
      } ~ appInfoRoutes
    )
  }

}
