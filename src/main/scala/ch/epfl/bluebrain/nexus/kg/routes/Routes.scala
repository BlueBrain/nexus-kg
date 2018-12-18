package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.kg.acls.AclsOps
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers
import ch.epfl.bluebrain.nexus.kg.marshallers.{ExceptionHandling, RejectionHandling}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

object Routes {

  /**
    * Generates the routes for all the platform resources
    *
    * @param resources the resources operations
    */
  def apply(resources: Resources[Task])(implicit cache: DistributedCache[Task],
                                        indexers: Clients[Task],
                                        store: FileStore[Task, AkkaIn, AkkaOut],
                                        aclsOps: AclsOps,
                                        config: AppConfig): Route = {
    import indexers._
    implicit val um = marshallers.sparqlQueryUnmarshaller

    (handleRejections(RejectionHandling()) & handleExceptions(ExceptionHandling())) {
      token { implicit optToken =>
        pathPrefix(config.http.prefix / Segment) { resourceSegment =>
          project.apply { implicit labeledProject =>
            (acls & caller) { (acl, c) =>
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
      }
    }
  }
}
