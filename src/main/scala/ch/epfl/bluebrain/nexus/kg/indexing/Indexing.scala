package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{untyped, UntypedHttpClient}
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticSearchView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.InProjectResolver
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.ResourceAlreadyExists
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.encoding._
import ch.epfl.bluebrain.nexus.sourcing.akka.Retry
import ch.epfl.bluebrain.nexus.sourcing.akka.syntax._
import com.github.ghik.silencer.silent
import io.circe.Json
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet

// $COVERAGE-OFF$
@silent
private class Indexing(
    resources: Resources[Task],
    cache: Caches[Task],
    adminClient: AdminClient[Task],
    coordinator: ProjectViewCoordinator[Task])(implicit mt: ActorMaterializer, as: ActorSystem, config: AppConfig) {

  private val logger                                          = Logger[this.type]
  private val http                                            = Http()
  private implicit val validation: AdditionalValidation[Task] = AdditionalValidation.pass
  private implicit val retry: Retry[Task, KgError] =
    Retry[Task, KgError](config.indexing.keyValueStore.retry.retryStrategy)

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

  def startAdminStream(): Unit = {

    def handle(event: Event): Task[Unit] = {
      logger.debug(s"Handling admin event: '$event'")
      event match {
        case OrganizationDeprecated(uuid, _, _, _) =>
          coordinator.stop(OrganizationRef(uuid))

        case ProjectCreated(uuid, label, orgUuid, orgLabel, desc, am, base, vocab, instant, subject) =>
          // format: off
          implicit val project: Project = Project(config.http.projectsIri + label, label, orgLabel, desc, base, vocab, am, uuid, orgUuid, 1L, deprecated = false, instant, subject.id, instant, subject.id)
          // format: on
          implicit val s: Identity.Subject = subject
          val elasticSearchView: View      = ElasticSearchView.default(project.ref)
          val sparqlView: View             = SparqlView.default(project.ref)
          val resolver: Resolver           = InProjectResolver.default(project.ref)
          // format: off
          cache.project.replace(project) *>
            coordinator.start(project) *>
            resources.create(Id(project.ref, elasticSearchView.id), viewRef, asJson(elasticSearchView)).value.mapRetry(createdOrExists, KgError.InternalError(s"Couldn't create default ElasticSearch view for project '${project.ref}'"): KgError) *>
            resources.create(Id(project.ref, sparqlView.id), viewRef, asJson(sparqlView)).value.mapRetry(createdOrExists, KgError.InternalError(s"Couldn't create default Sparql view for project '${project.ref}'"): KgError) *>
            resources.create(Id(project.ref, resolver.id), resolverRef, asJson(resolver)).value.mapRetry(createdOrExists, KgError.InternalError(s"Couldn't create default InProject resolver for project '${project.ref}'"): KgError) *>
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
    adminClient.events(handle)(config.iam.serviceAccountToken)
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
  def start(resources: Resources[Task], cache: Caches[Task], adminClient: AdminClient[Task])(
      implicit as: ActorSystem,
      ucl: HttpClient[Task, ResultSet],
      config: AppConfig): Unit = {
    implicit val mt: ActorMaterializer                          = ActorMaterializer()
    implicit val ul: UntypedHttpClient[Task]                    = untyped[Task]
    implicit val elasticSearchClient: ElasticSearchClient[Task] = ElasticSearchClient[Task](config.elasticSearch.base)

    val coordinatorRef = ProjectViewCoordinatorActor.start(resources, cache.view, None, config.cluster.shards)
    val coordinator    = new ProjectViewCoordinator[Task](cache, coordinatorRef)

    val indexing = new Indexing(resources, cache, adminClient, coordinator)
    indexing.startAdminStream()
    indexing.startResolverStream()
    indexing.startViewStream()
  }

}
// $COVERAGE-ON$
