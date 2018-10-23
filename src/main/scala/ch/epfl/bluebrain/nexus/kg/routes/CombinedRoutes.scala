package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.acls.AclsOps
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.QueryDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.marshallers.{ExceptionHandling, RejectionHandling}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes.{Schemed, Unschemed}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

object CombinedRoutes {

  /**
    * Generates the combined routes for all the platform resources
    *
    * @param resources the resources operations
    */
  def apply(resources: Resources[Task])(implicit cache: DistributedCache[Task],
                                        indexers: Clients[Task],
                                        aclsOps: AclsOps,
                                        store: AttachmentStore[Task, AkkaIn, AkkaOut],
                                        config: AppConfig): Route = {
    import indexers._
    implicit val um = marshallers.sparqlQueryUnmarshaller

    def listResources(implicit acl: FullAccessControlList, c: Caller, labeledProject: LabeledProject): Route = {
      val resourceRead = Permissions(Permission("resources/read"), Permission("resources/manage"))
      (get & parameter('deprecated.as[Boolean].?) & paginated & hasPermission(resourceRead) & pathEndOrSingleSlash) {
        (deprecated, pagination) =>
          trace("listResources") {
            complete(cache.views(labeledProject.ref).flatMap(v => resources.list(v, deprecated, pagination)).runAsync)
          }
      }
    }

    (handleRejections(RejectionHandling()) & handleExceptions(ExceptionHandling())) {
      token { implicit optToken =>
        pathPrefix(config.http.prefix) {
          // Resolvers
          (pathPrefix("resolvers") & project) { implicit labeledProject =>
            (acls & caller)(new ResolverRoutes(resources, _, _).routes)
          } ~ (pathPrefix("resources") & project) { implicit labeledProject =>
            (isIdSegment(resolverSchemaUri) & acls & caller)(new ResolverRoutes(resources, _, _).routes)
          } ~
            // Views
            (pathPrefix("views") & project) { implicit labeledProject =>
              (acls & caller)(new ViewRoutes(resources, _, _).routes)
            } ~ (pathPrefix("resources") & project) { implicit labeledProject =>
            (isIdSegment(viewSchemaUri) & acls & caller)(new ViewRoutes(resources, _, _).routes)
          } ~
            // Schemas
            (pathPrefix("schemas") & project) { implicit labeledProject =>
              (acls & caller)(new SchemaRoutes(resources, _, _).routes)
            } ~ (pathPrefix("resources") & project) { implicit labeledProject =>
            (isIdSegment(shaclSchemaUri) & acls & caller)(new SchemaRoutes(resources, _, _).routes)
          } ~
            // Resources
            (pathPrefix("resources") & project) { implicit labeledProject =>
              (acls & caller) { (acl, c) =>
                listResources(acl, c, labeledProject) ~
                  pathPrefix(IdSegment) { schema =>
                    new Schemed(resources, schema, "resources", acl, c).routes
                  }
              }
            } ~
            // Data
            (pathPrefix("data") & project) { implicit labeledProject =>
              (acls & caller) { (acl, c) =>
                listResources(acl, c, labeledProject) ~
                  new Unschemed(resources, "resources", acl, c).routes
              }
            }
        }
      }
    }
  }

}
