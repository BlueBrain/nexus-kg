package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.MediaTypes
import akka.http.scaladsl.model.headers.{`WWW-Authenticate`, HttpChallenges, Location}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route}
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure._
import ch.epfl.bluebrain.nexus.commons.http.directives.PrefixDirectives.uriPrefix
import ch.epfl.bluebrain.nexus.commons.http.{RdfMediaTypes, RejectionHandling}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlClientError
import ch.epfl.bluebrain.nexus.iam.client.IamClientError
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError._
import ch.epfl.bluebrain.nexus.kg.async.Caches._
import ch.epfl.bluebrain.nexus.kg.async.{Caches, ProjectCache, ProjectViewCoordinator, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.routes.AppInfoRoutes.HealthStatusGroup
import ch.epfl.bluebrain.nexus.kg.routes.HealthStatus._
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
    def completeGeneric(): Route =
      complete(InternalError("The system experienced an unexpected error, please try again later."): KgError)

    ExceptionHandler {
      case _: IamClientError.Unauthorized =>
        // suppress errors for authentication failures
        val status = KgError.kgErrorStatusFrom(AuthenticationFailed)
        val header = `WWW-Authenticate`(HttpChallenges.oAuth2("*"))
        complete((status, List(header), AuthenticationFailed: KgError))
      case _: IamClientError.Forbidden =>
        // suppress errors for authorization failures
        complete(AuthorizationFailed: KgError)
      case err: NotFound =>
        // suppress errors for not found
        complete((err: KgError))
      case AuthenticationFailed =>
        // suppress errors for authentication failures
        val status = KgError.kgErrorStatusFrom(AuthenticationFailed)
        val header = `WWW-Authenticate`(HttpChallenges.oAuth2("*"))
        complete((status, List(header), AuthenticationFailed: KgError))
      case AuthorizationFailed =>
        // suppress errors for authorization failures
        complete(AuthorizationFailed: KgError)
      case err: UnacceptedResponseContentType =>
        // suppress errors for unaccepted response content type
        complete(err: KgError)
      case err: ProjectNotFound =>
        // suppress error
        complete(err: KgError)
      case err: ProjectIsDeprecated =>
        // suppress error
        complete(err: KgError)
      case err: InvalidOutputFormat =>
        // suppress error
        complete(err: KgError)
      case ElasticSearchClientError(status, body) =>
        parse(body) match {
          case Right(json) => complete(status -> json)
          case Left(_)     => complete(status -> body)
        }
      case SparqlClientError(status, body) => complete(status -> body)
      case f: ElasticSearchFailure =>
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
    val custom = RejectionHandling.apply({ r: Rejection =>
      logger.debug(s"Handling rejection '$r'")
      r
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
      .withAllowedMethods(List(GET, PUT, POST, PATCH, DELETE, OPTIONS, HEAD))
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
  def apply(resources: Resources[Task], projectViewCoordinator: ProjectViewCoordinator[Task])(
      implicit as: ActorSystem,
      clients: Clients[Task],
      cache: Caches[Task],
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
      new ElasticSearchHealthStatus(clients.elasticSearch),
      new SparqlHealthStatus(clients.sparql)
    )
    val appInfoRoutes = AppInfoRoutes(config.description, healthStatusGroup).routes

    wrap(extractToken { implicit optToken =>
      (extractCallerAcls & extractCaller) { (acl, c) =>
        concat(
          pathPrefix(config.http.prefix / Segment) { resourceSegment =>
            project.apply {
              implicit project =>
                resourceSegment match {
                  case "resolvers" => new ResolverRoutes(resources, acl, c).routes
                  case "views"     => new ViewRoutes(resources, acl, c, projectViewCoordinator).routes
                  case "schemas"   => new SchemaRoutes(resources, acl, c).routes
                  case "files"     => new FileRoutes(resources, acl, c).routes
                  case "storages"  => new StorageRoutes(resources, acl, c).routes
                  case "resources" => new ResourceRoutes(resources, acl, c, projectViewCoordinator).routes
                  case "events"    => new EventRoutes(acl, c).routes
                  case _           => reject()
                }
            }
          },
          (pathPrefix(config.http.prefix / "events") & pathEndOrSingleSlash) { new GlobalEventRoutes(acl, c).routes }
        )
      }
    } ~ appInfoRoutes)
  }

}
