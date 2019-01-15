package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerSettings
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.admin.client.types.events.decoders._
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.kg.async.{DistributedCache, ProjectViewCoordinator}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SingleView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectRef, Resources}
import ch.epfl.bluebrain.nexus.service.kafka.KafkaConsumer
import com.github.ghik.silencer.silent
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future

// $COVERAGE-OFF$
private class Indexing(resources: Resources[Task], cache: DistributedCache[Task], @silent coordinator: ActorRef)(
    implicit as: ActorSystem,
    config: AppConfig) {
  private val consumerSettings = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)

//  private val elasticUUID = UUID.fromString("684bd815-9273-46f4-ac1c-0383d4a98254")
//  private val sparqlUUID  = UUID.fromString("d88b71d2-b8a4-4744-bf22-2d99ef5bd26b")

  private val orgsPrefix     = config.http.baseIri + "orgs"
  private val projectsPrefix = config.http.baseIri + "projects"

//  private val defaultEsMapping =
//    jsonContentOf("/elastic/mapping.json", Map(Pattern.quote("{{docType}}") -> config.elastic.docType))

  def startKafkaStream(): Unit = {

// TODO:
//    def defaultEsView(projectRef: ProjectRef): ElasticView =
//      ElasticView(defaultEsMapping,
//                  Set.empty,
//                  None,
//                  includeMetadata = true,
//                  sourceAsText = true,
//                  projectRef,
//                  nxv.defaultElasticIndex.value,
//                  elasticUUID,
//                  1L,
//                  deprecated = false)
//
//    def defaultSparqlView(projectRef: ProjectRef): SparqlView =
//      SparqlView(projectRef, nxv.defaultSparqlIndex.value, sparqlUUID, 1L, deprecated = false)
//
//    def defaultInProjectResolver(projectRef: ProjectRef): InProjectResolver =
//      InProjectResolver(projectRef, nxv.InProject.value, 1L, deprecated = false, 1)

    implicit val icc: IamClientConfig = config.iam.iamClient

    def index(event: Event): Future[Unit] = {

      val update = event match {
        case OrganizationCreated(uuid, label, desc, instant, subject) =>
          cache.addOrganization(OrganizationRef(uuid),
                                Organization(orgsPrefix + label,
                                             label,
                                             desc,
                                             uuid,
                                             1L,
                                             deprecated = false,
                                             instant,
                                             subject.id,
                                             instant,
                                             subject.id))
        case OrganizationUpdated(uuid, rev, label, desc, instant, subject) =>
          cache.addOrganization(OrganizationRef(uuid),
                                Organization(orgsPrefix + label,
                                             label,
                                             desc,
                                             uuid,
                                             rev,
                                             deprecated = false,
                                             instant,
                                             subject.id,
                                             instant,
                                             subject.id))
        case OrganizationDeprecated(uuid, rev, _, _) =>
          cache.deprecateOrganization(OrganizationRef(uuid), rev)
        case ProjectCreated(uuid, label, orgUuid, orgLabel, desc, am, base, vocab, instant, subject) =>
          val projectRef = ProjectRef(uuid)
          val orgRef     = OrganizationRef(uuid)
          cache.addProject(
            projectRef,
            orgRef,
            Project(projectsPrefix + label,
                    label,
                    orgLabel,
                    desc,
                    base,
                    vocab,
                    am,
                    uuid,
                    orgUuid,
                    1L,
                    deprecated = false,
                    instant,
                    subject.id,
                    instant,
                    subject.id)
          )
        case ProjectUpdated(uuid, label, desc, am, base, vocab, rev, instant, subject) =>
          cache.project(ProjectRef(uuid)).flatMap {
            case Some(project) =>
              cache.addProject(
                ProjectRef(uuid),
                OrganizationRef(project.organizationUuid),
                Project(
                  projectsPrefix + label,
                  label,
                  project.organizationLabel,
                  desc,
                  base,
                  vocab,
                  am,
                  uuid,
                  project.organizationUuid,
                  rev,
                  deprecated = false,
                  instant,
                  subject.id,
                  instant,
                  subject.id
                )
              )
            case None => Task.unit
          }
        case ProjectDeprecated(uuid, rev, _, _) =>
          cache.project(ProjectRef(uuid)).flatMap {
            case Some(project) =>
              cache.deprecateProject(ProjectRef(uuid), OrganizationRef(project.organizationUuid), rev)
            case None => Task.unit
          }
      }
      update.runToFuture
    }

    KafkaConsumer.start(consumerSettings, index, config.kafka.adminTopic, "admin-events", committable = false, None)
    ()
  }

  def startResolverStream(): Unit = {
    ResolverIndexer.start(resources, cache)
    ()
  }

  def startViewStream(): Unit = {
    ViewIndexer.start(resources, cache)
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
  def start(resources: Resources[Task], cache: DistributedCache[Task])(implicit as: ActorSystem,
                                                                       ucl: HttpClient[Task, ResultSet],
                                                                       config: AppConfig): Unit = {

    implicit val mt            = ActorMaterializer()
    implicit val ul            = untyped[Task]
    implicit val jsonClient    = withUnmarshaller[Task, Json]
    implicit val elasticClient = ElasticClient[Task](config.elastic.base)

    def selector(view: SingleView, project: Project): ActorRef = view match {
      case v: ElasticView => ElasticIndexer.start(v, resources, project)
      case v: SparqlView  => SparqlIndexer.start(v, resources, project)
    }

    def onStop(view: SingleView): Task[Boolean] = view match {
      case v: ElasticView =>
        elasticClient.deleteIndex(v.index)
      case _: SparqlView =>
        BlazegraphClient[Task](config.sparql.base, view.name, config.sparql.akkaCredentials).deleteNamespace
    }

    val coordinator = ProjectViewCoordinator.start(cache, selector, onStop, None, config.cluster.shards)
    val indexing    = new Indexing(resources, cache, coordinator)
    //    indexing.startKafkaStream()
    indexing.startResolverStream()
    indexing.startViewStream()
  }

}
// $COVERAGE-ON$
