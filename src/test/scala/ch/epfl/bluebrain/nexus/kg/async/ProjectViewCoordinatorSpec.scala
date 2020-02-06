package ch.epfl.bluebrain.nexus.kg.async

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Props
import akka.persistence.query.Sequence
import akka.testkit.DefaultTimeout
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.commons.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, User}
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlList, AccessControlLists}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.archives.ArchiveCache
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.{onViewChange, ViewCoordinator}
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Projection.{ElasticSearchProjection, SparqlProjection}
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Source.{CrossProjectEventStream, ProjectEventStream}
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.resources.OrganizationRef
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.sourcing.projections.ProjectionProgress.OffsetProgress
import ch.epfl.bluebrain.nexus.sourcing.projections.{ProjectionProgress, Projections, StreamSupervisor}
import com.typesafe.scalalogging.Logger
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{Inspectors, OptionValues}

import scala.concurrent.duration._

class ProjectViewCoordinatorSpec
    extends ActorSystemFixture("ProjectViewCoordinatorSpec", true)
    with TestHelper
    with DefaultTimeout
    with AnyWordSpecLike
    with Matchers
    with Eventually
    with ScalaFutures
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with Inspectors
    with OptionValues {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(15.second, 150.milliseconds)

  private implicit val appConfig    = Settings(system).appConfig
  private implicit val log: Logger  = Logger[this.type]
  private implicit val projectCache = ProjectCache[Task]
  private val viewCache             = ViewCache[Task]

  "A ProjectViewCoordinator" should {
    val creator = genIri

    val orgUuid = genUUID
    // format: off
    implicit val project  = Project(genIri, "some-project", "some-org", None, genIri, genIri, Map.empty, genUUID, orgUuid, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val project2          = Project(genIri, "some-project2", "some-org", None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, deprecated = false, Instant.EPOCH, creator, Instant.EPOCH, creator)
    val projectLabelUpdated = genString()
    val project2Updated   = project2.copy(label = projectLabelUpdated, rev = 2L)
    val view              = SparqlView(Filter(), true, project.ref, genIri, genUUID, 1L, deprecated = false)
    val view2             = ElasticSearchView(Json.obj(), Filter(Set(genIri)), true, true, project.ref, genIri, genUUID, 1L, deprecated = false)
    val view2Updated      = view2.copy(filter = view2.filter.copy(resourceSchemas = Set(genIri)), rev = 2L)
    val view3             = SparqlView(Filter(), true, project2.ref, genIri, genUUID, 1L, deprecated = false)
    val projection1       = ElasticSearchProjection("query", ElasticSearchView(Json.obj(), Filter(), false, false, project.ref, genIri, genUUID, 1L, false), Json.obj())
    val projection2       = SparqlProjection("query2", SparqlView(Filter(), true, project.ref, genIri, genUUID, 1L, false))
    val localS            = ProjectEventStream(genIri, genUUID, Filter())
    val crossProjectS     = CrossProjectEventStream(genIri, genUUID, Filter(), project2.ref, Set(Anonymous))
    val view4             = CompositeView(Set(localS, crossProjectS), Set(projection1, projection2), None, project.ref, genIri, genUUID, 1L, false)
    // format: on

    val counterStart            = new AtomicInteger(0)
    val counterStartProjections = new AtomicInteger(0)
    val counterStop             = new AtomicInteger(0)

    val coordinator1         = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator2         = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator2Updated  = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator3         = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator3Updated  = mock[StreamSupervisor[Task, ProjectionProgress]]
    val coordinator4         = mock[StreamSupervisor[Task, ProjectionProgress]]
    implicit val projections = mock[Projections[Task, String]]
    implicit val aclsCache   = mock[AclsCache[Task]]

    coordinator1.stop() shouldReturn Task.unit
    coordinator2.stop() shouldReturn Task.unit
    coordinator2Updated.stop() shouldReturn Task.unit
    coordinator3.stop() shouldReturn Task.unit
    coordinator3Updated.stop() shouldReturn Task.unit
    coordinator4.stop() shouldReturn Task.unit

    val coordinatorProps = Props(
      new ProjectViewCoordinatorActor(viewCache) {
        override def startCoordinator(
            v: View.IndexedView,
            proj: Project,
            restart: Boolean,
            prevRestart: Option[Instant]
        ): ViewCoordinator = {
          counterStart.incrementAndGet()
          if (v == view && proj == project) ViewCoordinator(coordinator1)
          else if (v == view2 && proj == project) ViewCoordinator(coordinator2)
          else if (v == view2Updated && proj == project) ViewCoordinator(coordinator2Updated)
          else if (v == view3 && proj == project2) ViewCoordinator(coordinator3)
          else if (v == view3.copy(rev = 2L) && proj == project2) ViewCoordinator(coordinator3)
          else if (v == view3.copy(rev = 2L) && proj == project2Updated && restart)
            ViewCoordinator(coordinator3Updated)
          else if (v == view4 && proj == project) ViewCoordinator(coordinator4)
          else if (v == view4.copy(sources = Set(localS)) && proj == project) ViewCoordinator(coordinator4)
          else throw new RuntimeException()
        }

        override def startCoordinator(
            view: CompositeView,
            proj: Project,
            restartProgress: Set[String],
            prevRestart: Option[Instant]
        ): ViewCoordinator =
          if (view == view4 && proj == project) {
            counterStartProjections.incrementAndGet()
            ViewCoordinator(coordinator4)
          } else throw new RuntimeException()

        override def deleteViewIndices(view: View.IndexedView, project: Project): Task[Unit] = {
          counterStop.incrementAndGet()
          Task.unit
        }

        override def onChange(ref: ProjectRef): OnKeyValueStoreChange[AbsoluteIri, View] =
          onViewChange(ref, self)

      }
    )

    val coordinatorRef = ProjectViewCoordinatorActor.start(coordinatorProps, None, 1)
    val coordinator =
      new ProjectViewCoordinator[Task](
        Caches(
          projectCache,
          viewCache,
          mock[ResolverCache[Task]],
          mock[StorageCache[Task]],
          mock[ArchiveCache[Task]]
        ),
        coordinatorRef
      )

    projections.progress(any[String]) shouldReturn Task.pure(ProjectionProgress.NoProgress)

    val currentStart     = new AtomicInteger(0)
    val currentProjStart = new AtomicInteger(0)
    val currentStop      = new AtomicInteger(0)

    "initialize projects" in {
      projectCache.replace(project).runToFuture.futureValue
      projectCache.replace(project2).runToFuture.futureValue

      coordinator.start(project).runToFuture.futureValue
      eventually(counterStart.get shouldEqual currentStart.get)

      coordinator.start(project2).runToFuture.futureValue
      eventually(counterStart.get shouldEqual currentStart.get)
    }

    "start view indexer when views are cached" in {
      viewCache.put(view2).runToFuture.futureValue
      currentStart.incrementAndGet()
      eventually(counterStart.get shouldEqual currentStart.get)

      currentStart.incrementAndGet()
      viewCache.put(view).runToFuture.futureValue
      eventually(counterStart.get shouldEqual currentStart.get)

      currentStart.incrementAndGet()
      viewCache.put(view3).runToFuture.futureValue
      eventually(counterStart.get shouldEqual currentStart.get)

      currentStart.incrementAndGet()
      aclsCache.list shouldReturn
        Task(AccessControlLists(/ -> resourceAcls(AccessControlList(Anonymous -> Set(read)))))
      viewCache.put(view4).runToFuture.futureValue
      eventually(counterStart.get shouldEqual currentStart.get)

      counterStartProjections.get shouldEqual currentProjStart.get
      counterStop.get shouldEqual counterStop.get
    }

    "fetch statistics" in {
      val progress: ProjectionProgress = OffsetProgress(Sequence(2L), 2L, 0L, 1L)
      coordinator1.state() shouldReturn Task(Some(progress))
      val result = coordinator.statistics(view.id, None).runToFuture.futureValue.value.head.value
      result.processedEvents shouldEqual 2L
      result.discardedEvents shouldEqual 0L
      result.failedEvents shouldEqual 1L
    }

    "do not when coordinator not present fetch statistics" in {
      coordinator3.state() shouldReturn Task(None)
      coordinator.projectionStats(view3.id, None, None).runToFuture.futureValue shouldEqual None
    }

    "fetch projections statistics" in {
      val progress: ProjectionProgress = OffsetProgress(Sequence(2L), 2L, 0L, 1L)
      coordinator4.state() shouldReturn Task(Some(progress))
      val result = coordinator.projectionStats(view4.id, None, None).runToFuture.futureValue.value
      forAll(result.map(_.value)) { statistic =>
        statistic.processedEvents shouldEqual 2L
        statistic.discardedEvents shouldEqual 0L
        statistic.failedEvents shouldEqual 1L
      }
    }

    "fetch offset" in {
      val progress: ProjectionProgress = OffsetProgress(Sequence(2L), 2L, 0L, 1L)
      coordinator2.state() shouldReturn Task(Some(progress))
      coordinator.offset(view2.id, None).runToFuture.futureValue.value.head.value shouldEqual Sequence(2L)
    }

    "fetch projections offset" in {
      val progress: ProjectionProgress = OffsetProgress(Sequence(2L), 2L, 0L, 1L)
      coordinator4.state() shouldReturn Task(Some(progress))
      val result = coordinator.projectionOffset(view4.id, None, None).runToFuture.futureValue.value
      result.iterator.size shouldEqual 4L
      forAll(result.map(_.value)) { offset =>
        offset shouldEqual Sequence(2L)
      }
    }

    "trigger manual view restart" in {
      coordinator.restart(view.id).runToFuture.futureValue
      currentStart.incrementAndGet()
      eventually(coordinator1.stop() wasCalled once)
      eventually(counterStart.get shouldEqual currentStart.get)
      eventually(counterStop.get shouldEqual currentStop.get)
      counterStartProjections.get shouldEqual currentProjStart.get
    }

    "trigger manual projections restart" in {
      coordinator.restart(view4.id, None, None).runToFuture.futureValue
      currentProjStart.incrementAndGet()
      eventually(coordinator4.stop() wasCalled once)
      eventually(counterStartProjections.get shouldEqual currentProjStart.get)
      counterStart.get shouldEqual currentStart.get
      counterStop.get shouldEqual currentStop.get
    }

    "stop view when view is removed (deprecated) from the cache" in {
      viewCache.put(view.copy(deprecated = true)).runToFuture.futureValue
      currentStop.incrementAndGet()
      eventually(coordinator1.stop() wasCalled twice)
      eventually(counterStop.get shouldEqual currentStop.get)
      eventually(counterStart.get shouldEqual currentStart.get)
      counterStartProjections.get shouldEqual currentProjStart.get
    }

    "stop old elasticsearch view start new view when current view updated" in {
      viewCache.put(view2Updated).runToFuture.futureValue
      currentStop.incrementAndGet()
      eventually(counterStop.get shouldEqual currentStop.get)

      currentStart.incrementAndGet()
      eventually(coordinator2.stop() wasCalled once)
      eventually(counterStart.get shouldEqual currentStart.get)

      counterStartProjections.get shouldEqual currentProjStart.get
    }

    "stop old sparql view start new view when current view updated" in {
      viewCache.put(view3.copy(rev = 2L)).runToFuture.futureValue
      currentStop.incrementAndGet()
      eventually(counterStop.get shouldEqual currentStop.get)

      currentStart.incrementAndGet()
      eventually(coordinator3.stop() wasCalled once)
      eventually(counterStart.get shouldEqual currentStart.get)
      counterStartProjections.get shouldEqual currentProjStart.get
    }

    "stop all related views when organization is deprecated" in {
      coordinator.stop(OrganizationRef(orgUuid)).runToFuture.futureValue
      eventually(coordinator2Updated.stop() wasCalled once)
      eventually(counterStop.get shouldEqual currentStop.get)
      eventually(counterStart.get shouldEqual currentStart.get)
      counterStartProjections.get shouldEqual currentProjStart.get
    }

    "restart all related views when project changes" in {
      projectCache.replace(project2Updated).runToFuture.futureValue
      coordinator.change(project2Updated, project2).runToFuture.futureValue
      currentStop.incrementAndGet()
      eventually(counterStop.get shouldEqual currentStop.get)
      currentStart.incrementAndGet()
      eventually(counterStart.get shouldEqual currentStart.get)
      eventually(coordinator3.stop() wasCalled twice)
      counterStartProjections.get shouldEqual currentProjStart.get
    }

    "stop related views when project is deprecated" in {
      projectCache.replace(project2Updated.copy(deprecated = true)).runToFuture.futureValue
      coordinator.stop(project2Updated.ref).runToFuture.futureValue
      eventually(counterStop.get shouldEqual currentStop.get)
      eventually(counterStart.get shouldEqual currentStart.get)
      eventually(coordinator3Updated.stop() wasCalled once)
      counterStartProjections.get shouldEqual currentProjStart.get
    }

    "restart CompositeView on ACLs change" in {
      val projectPath = project.organizationLabel / project.label
      val acls = AccessControlLists(
        projectPath -> resourceAcls(AccessControlList(Anonymous                      -> Set(read, write))),
        /           -> resourceAcls(AccessControlList(User(genString(), genString()) -> Set(read, write)))
      )

      coordinator.changeAcls(acls, project).runToFuture.futureValue
      eventually(coordinator4.stop() wasCalled twice)
      currentStart.incrementAndGet()
      eventually(counterStart.get shouldEqual currentStart.get)
    }

    "do nothing when the ACL changes do not affect the triggered project" in {
      val projectPath = project.organizationLabel / project.label
      val acls = AccessControlLists(
        projectPath -> resourceAcls(AccessControlList(Anonymous                      -> Set(read, write))),
        /           -> resourceAcls(AccessControlList(User(genString(), genString()) -> Set(read, write)))
      )

      coordinator.changeAcls(acls, project2).runToFuture.futureValue
      coordinator4.stop() wasCalled twice
      counterStart.get shouldEqual currentStart.get
    }

    "resolve projects from path" in {
      val paths = List(
        /                           -> Set(project, project2Updated),
        / + "some-org"              -> Set(project, project2Updated),
        "some-org" / "some-project" -> Set(project)
      )
      forAll(paths) {
        case (path, projects) => path.resolveProjects.runToFuture.futureValue.toSet shouldEqual projects
      }
    }
  }
}
