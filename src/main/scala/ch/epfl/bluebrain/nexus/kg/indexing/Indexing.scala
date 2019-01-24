package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.sse.scaladsl.EventSource
import akka.stream.scaladsl.Sink
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event._
import ch.epfl.bluebrain.nexus.admin.client.types.events.decoders._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.untyped
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.InProjectResolver
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.ResourceAlreadyExists
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.syntax.akka._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.Backoff
import ch.epfl.bluebrain.nexus.service.indexer.retryer.syntax._
import com.github.ghik.silencer.silent
import io.circe.Json
import io.circe.parser._
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet

import scala.concurrent.Future
import scala.concurrent.duration._

// $COVERAGE-OFF$
@silent
private class Indexing(resources: Resources[Task], cache: Caches[Task], coordinator: ProjectViewCoordinator[Task])(
    implicit mt: ActorMaterializer,
    as: ActorSystem,
    config: AppConfig) {

  private val logger                                          = Logger[this.type]
  private val http                                            = Http()
  private implicit val validation: AdditionalValidation[Task] = AdditionalValidation.pass
  private implicit val strategy: RetryStrategy                = Backoff(1 minute, 0.2)

  private def asJson(view: View): Json =
    view
      .asJson(viewCtx.appendContextOf(resourceCtx))
      .removeKeys("@context", nxv.rev.prefix, nxv.deprecated.prefix)
      .addContext(viewCtxUri)
      .addContext(resourceCtxUri)

  private def asJson(resolver: Resolver): Json =
    resolver
      .asJson(resolverCtx.appendContextOf(resourceCtx))
      .removeKeys("@context", nxv.rev.prefix, nxv.deprecated.prefix)
      .addContext(resolverCtxUri)
      .addContext(resourceCtxUri)

  private val createdOrExists: PartialFunction[Either[Rejection, Resource], Either[ResourceAlreadyExists, Resource]] = {
    case Left(exists: ResourceAlreadyExists) => Left(exists)
    case Right(value)                        => Right(value)
  }

  private def addCredentials(request: HttpRequest): HttpRequest =
    config.iam.serviceAccountToken
      .map(token => request.addCredentials(OAuth2BearerToken(token.value)))
      .getOrElse(request)

  private def send(request: HttpRequest): Future[HttpResponse] = {
    http.singleRequest(addCredentials(request)).map { resp =>
      if (!resp.status.isSuccess())
        logger.warn(s"HTTP response when performing SSE request: status = '${resp.status}'")
      resp
    }
  }

  def startAdminStream(): Unit = {

    def handle(event: Event): Task[Unit] = {
      logger.debug(s"Handling admin event: '$event'")
      event match {
        case OrganizationDeprecated(uuid, _, _, _) =>
          coordinator.stop(OrganizationRef(uuid))

        case ProjectCreated(uuid, label, orgUuid, orgLabel, desc, am, base, vocab, instant, subject) =>
          // format: off
          implicit val project = Project(config.http.projectsIri + label, label, orgLabel, desc, base, vocab, am, uuid, orgUuid, 1L, deprecated = false, instant, subject.id, instant, subject.id)
          // format: on
          implicit val s         = subject
          val elasticView: View  = ElasticView.default(project.ref)
          val sparqlView: View   = SparqlView.default(project.ref)
          val resolver: Resolver = InProjectResolver.default(project.ref)
          // format: off
          cache.project.replace(project) *>
            coordinator.start(project) *>
            resources.create(Id(project.ref, elasticView.id), viewRef, asJson(elasticView)).value.retryWhenNot(createdOrExists, 15) *>
            resources.create(Id(project.ref, sparqlView.id), viewRef, asJson(sparqlView)).value.retryWhenNot(createdOrExists, 15) *>
            resources.create(Id(project.ref, resolver.id), resolverRef, asJson(resolver)).value.retryWhenNot(createdOrExists, 15) *>
            Task.unit
          // format: on

        case ProjectUpdated(uuid, label, desc, am, base, vocab, rev, instant, subject) =>
          cache.project.get(ProjectRef(uuid)).flatMap {
            case Some(project) =>
              // format: off
              val newProject = Project(config.http.projectsIri + label, label, project.organizationLabel, desc, base, vocab, am, uuid, project.organizationUuid, rev, deprecated = false, instant, subject.id, instant, subject.id)
              // format: on
              cache.project.replace(newProject) *> coordinator.change(newProject, project)
            case None => Task.unit
          }
        case ProjectDeprecated(uuid, rev, _, _) =>
          cache.project.deprecate(ProjectRef(uuid), rev) *> coordinator.stop(ProjectRef(uuid))
        case _ => Task.unit
      }
    }

    EventSource((config.admin.baseUri + "events").toAkkaUri, send, None, 1 second)
      .mapAsync(1) { sse =>
        decode[Event](sse.data) match {
          case Right(event) => handle(event).runToFuture
          case Left(err) =>
            logger.error(s"Failed to decode admin event '$sse'", err)
            Future.unit
        }
      }
      .to(Sink.ignore)
      .run()
    ()
  }

  def startResolverStream(): Unit = {
    ResolverIndexer.start(resources, cache.resolver, cache.project)
    ()
  }

  def startViewStream(): Unit = {
    ViewIndexer.start(resources, cache.view, cache.project)
    ()
  }
}

object Indexing {

  /**
    * Starts all indexing streams:
    * <ul>
    *   <li>Views</li>
    *   <li>Projects</li>
    *   <li>Accounts</li>
    *   <li>Resolvers</li>
    * </ul>
    *
    * @param resources the resources operations
    * @param cache     the distributed cache
    */
  def start(resources: Resources[Task], cache: Caches[Task])(implicit as: ActorSystem,
                                                             ucl: HttpClient[Task, ResultSet],
                                                             config: AppConfig): Unit = {
    implicit val mt            = ActorMaterializer()
    implicit val ul            = untyped[Task]
    implicit val elasticClient = ElasticClient[Task](config.elastic.base)

    val coordinatorRef = ProjectViewCoordinatorActor.start(resources, cache.view, None, config.cluster.shards)
    val coordinator    = new ProjectViewCoordinator[Task](cache, coordinatorRef)

    val indexing = new Indexing(resources, cache, coordinator)
    indexing.startAdminStream()
    indexing.startResolverStream()
    indexing.startViewStream()
  }

}
// $COVERAGE-ON$
