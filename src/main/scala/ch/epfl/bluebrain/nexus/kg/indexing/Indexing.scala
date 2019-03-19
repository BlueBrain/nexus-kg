package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{untyped, UntypedHttpClient}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlResults
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.async.Caches._
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
import ch.epfl.bluebrain.nexus.kg.storage.Storage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.DiskStorage
import ch.epfl.bluebrain.nexus.kg.storage.StorageEncoder._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.sourcing.retry.Retry
import ch.epfl.bluebrain.nexus.sourcing.retry.syntax._
import io.circe.Json
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

// $COVERAGE-OFF$
private class Indexing(
    resources: Resources[Task],
    adminClient: AdminClient[Task],
    coordinator: ProjectViewCoordinator[Task])(implicit cache: Caches[Task], config: AppConfig, as: ActorSystem) {

  private val logger                                          = Logger[this.type]
  private implicit val validation: AdditionalValidation[Task] = AdditionalValidation.pass
  private implicit val retry: Retry[Task, KgError] =
    Retry[Task, KgError](config.keyValueStore.indexing.retry.retryStrategy)

  private def asJson(view: View): Task[Json] =
    view.as[Json](viewCtx.appendContextOf(resourceCtx)) match {
      case Left(err) =>
        logger.error(s"Could not convert view with id '${view.id}' from Graph back to json. Reason: '${err.message}'")
        Task.raiseError(InternalError("Could not decode default view from graph to Json"))
      case Right(json) =>
        val jsonNoMeta = json.removeKeys(nxv.rev.prefix, nxv.deprecated.prefix)
        Task.pure(jsonNoMeta.replaceContext(viewCtxUri).addContext(resourceCtxUri))
    }

  private def asJson(storage: Storage): Task[Json] =
    storage.as[Json](storageCtx.appendContextOf(resourceCtx)) match {
      case Left(err) =>
        logger.error(s"Could not convert storage '${storage.id}' from Graph to json. Reason: '${err.message}'")
        Task.raiseError(InternalError("Could not decode default storage from graph to Json"))
      case Right(json) =>
        val jsonNoMeta = json.removeKeys(nxv.rev.prefix, nxv.deprecated.prefix, nxv.algorithm.prefix)
        Task.pure(jsonNoMeta.replaceContext(storageCtxUri).addContext(resourceCtxUri))
    }

  private def asJson(resolver: Resolver): Task[Json] =
    resolver.as[Json](resolverCtx.appendContextOf(resourceCtx)) match {
      case Left(err) =>
        logger.error(s"Could not convert resolver '${resolver.id}' from Graph to json. Reason: '${err.message}'")
        Task.raiseError(InternalError("Could not decode default in project resolver from graph to Json"))
      case Right(json) =>
        val jsonNoMeta = json.removeKeys(nxv.rev.prefix, nxv.deprecated.prefix)
        Task.pure(jsonNoMeta.replaceContext(resolverCtxUri).addContext(resourceCtxUri))
    }

  private val createdOrExists: PartialFunction[Either[Rejection, Resource], Either[ResourceAlreadyExists, Resource]] = {
    case Left(exists: ResourceAlreadyExists) => Left(exists)
    case Right(value)                        => Right(value)
  }

  private def createElasticSearchView(implicit project: Project, s: Subject): Task[Either[Rejection, Resource]] = {
    val view: View = ElasticSearchView.default(project.ref)
    asJson(view).flatMap { json =>
      val created = resources.create(Id(project.ref, view.id), viewRef, json).value
      created.mapRetry(
        createdOrExists,
        InternalError(s"Couldn't create default ElasticSearch view for project '${project.ref}'"): KgError)
    }
  }

  private def createSparqlView(implicit project: Project, s: Subject): Task[Either[Rejection, Resource]] = {
    val view: View = SparqlView.default(project.ref)
    asJson(view).flatMap { json =>
      val created = resources.create(Id(project.ref, view.id), viewRef, json).value
      created.mapRetry(createdOrExists,
                       InternalError(s"Couldn't create default Sparql view for project '${project.ref}'"): KgError)
    }
  }

  private def createResolver(implicit project: Project, s: Subject): Task[Either[Rejection, Resource]] = {
    val resolver: Resolver = InProjectResolver.default(project.ref)
    asJson(resolver).flatMap { json =>
      val created = resources.create(Id(project.ref, resolver.id), resolverRef, json).value
      created.mapRetry(
        createdOrExists,
        InternalError(s"Couldn't create default InProject resolver for project '${project.ref}'"): KgError)
    }
  }

  private def createDiskStorage(implicit project: Project, s: Subject): Task[Either[Rejection, Resource]] = {
    val storage: Storage = DiskStorage.default(project.ref)
    asJson(storage).flatMap { json =>
      val created = resources.create(Id(project.ref, storage.id), storageRef, json).value
      created.mapRetry(createdOrExists,
                       InternalError(s"Couldn't create default DiskStorage for project '${project.ref}'"): KgError)
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
          implicit val project: Project = Project(config.http.projectsIri + label, label, orgLabel, desc, base, vocab, am, uuid, orgUuid, 1L, deprecated = false, instant, subject.id, instant, subject.id)
          // format: on
          implicit val s: Identity.Subject = subject
          for {
            _ <- cache.project.replace(project)
            _ <- coordinator.start(project)
            _ <- List(createElasticSearchView, createSparqlView, createResolver, createDiskStorage).sequence
          } yield (())

        case ProjectUpdated(uuid, label, desc, am, base, vocab, rev, instant, subject) =>
          cache.project.get(ProjectRef(uuid)).flatMap {
            case Some(project) =>
              // format: off
              val newProject = Project(config.http.projectsIri + label, label, project.organizationLabel, desc, base, vocab, am, uuid, project.organizationUuid, rev, deprecated = false, instant, subject.id, instant, subject.id)
              // format: on
              cache.project.replace(newProject).flatMap(_ => coordinator.change(newProject, project))
            case None => Task.unit
          }
        case ProjectDeprecated(uuid, rev, _, _) =>
          cache.project.deprecate(ProjectRef(uuid), rev).flatMap(_ => coordinator.stop(ProjectRef(uuid)))
        case _ => Task.unit
      }
    }
    adminClient.events(handle)(config.iam.serviceAccountToken)
  }

  def startResolverStream(): Unit = {
    ResolverIndexer.start(resources, cache.resolver)
    ()
  }

  def startViewStream(): Unit = {
    ViewIndexer.start(resources, cache.view)
    ()
  }

  def startStorageStream(): Unit = {
    StorageIndexer.start(resources, cache.storage)
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
  def start(resources: Resources[Task], adminClient: AdminClient[Task])(
      implicit cache: Caches[Task],
      config: AppConfig,
      as: ActorSystem,
      ucl: HttpClient[Task, SparqlResults]): ProjectViewCoordinator[Task] = {
    implicit val mt: ActorMaterializer                          = ActorMaterializer()
    implicit val ul: UntypedHttpClient[Task]                    = untyped[Task]
    implicit val elasticSearchClient: ElasticSearchClient[Task] = ElasticSearchClient[Task](config.elasticSearch.base)

    val coordinatorRef = ProjectViewCoordinatorActor.start(resources, cache.view, None, config.cluster.shards)
    val coordinator    = new ProjectViewCoordinator[Task](cache, coordinatorRef)

    val indexing = new Indexing(resources, adminClient, coordinator)
    indexing.startAdminStream()
    indexing.startResolverStream()
    indexing.startViewStream()
    indexing.startStorageStream()
    coordinator
  }

}
// $COVERAGE-ON$
