package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticServerError
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, CirceEq, Randomness}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.{KgError, TestHelper}
import ch.epfl.bluebrain.nexus.kg.cache.{AclsCache, ProjectCache, ResolverCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resolve.{Materializer, ProjectResolution, Resolver, StaticResolution}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Iri, RootedGraph}
import io.circe.Json
import org.mockito.ArgumentMatchers.any
import org.scalatest._
import org.mockito.ArgumentMatchers.{eq => mEq}
import io.circe.parser.parse
import org.mockito.{IdiomaticMockito, Mockito}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

//noinspection TypeAnnotation
class ViewsSpec
    extends ActorSystemFixture("ViewsSpec", true)
    with IOEitherValues
    with IOOptionValues
    with WordSpecLike
    with IdiomaticMockito
    with Matchers
    with OptionValues
    with EitherValues
    with Randomness
    with test.Resources
    with BeforeAndAfter
    with TestHelper
    with Inspectors
    with CirceEq {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3 second, 15 milliseconds)

  private implicit val appConfig              = Settings(system).appConfig
  private implicit val clock: Clock           = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

  private implicit val repo          = Repo[IO].ioValue
  private implicit val projectCache  = mock[ProjectCache[IO]]
  private implicit val viewCache     = mock[ViewCache[IO]]
  private implicit val esClient      = mock[ElasticSearchClient[IO]]
  private val aclsCache              = mock[AclsCache[IO]]
  private implicit val resolverCache = mock[ResolverCache[IO]]

  private val resolution =
    new ProjectResolution(resolverCache,
                          mock[ProjectCache[IO]],
                          StaticResolution[IO](iriResolution),
                          mock[AclsCache[IO]])
  private implicit val materializer = new Materializer[IO](repo, resolution)

  resolverCache.get(any[ProjectRef]) shouldReturn IO.pure(List.empty[Resolver])
  // format: off
  val project1 = Project(genIri, genString(), genString(), None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, deprecated = false, Instant.EPOCH, genIri, Instant.EPOCH, genIri)
  val project2 = Project(genIri, genString(), genString(), None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, deprecated = false, Instant.EPOCH, genIri, Instant.EPOCH, genIri)
  // format: on
  projectCache.getBy(ProjectLabel("account1", "project1")) shouldReturn IO.pure(Some(project1))
  projectCache.getBy(ProjectLabel("account1", "project2")) shouldReturn IO.pure(Some(project2))
  val label1 = ProjectLabel("account1", "project1")
  val label2 = ProjectLabel("account1", "project2")
  projectCache.getProjectRefs(Set(label1, label2)) shouldReturn IO.pure(
    Map(label1 -> Option(project1.ref), label2 -> Option(project2.ref)))
  viewCache.get(project1.ref) shouldReturn IO.pure(
    Set[View](ElasticSearchView.default(project1.ref).copy(id = url"http://example.com/id2".value)))
  viewCache.get(project2.ref) shouldReturn IO.pure(
    Set[View](ElasticSearchView.default(project2.ref).copy(id = url"http://example.com/id3".value)))

  private val views: Views[IO] = Views[IO]

  before {
    Mockito.reset(resolverCache)
  }

  trait Base {
    implicit val caller = Caller(Anonymous, Set(Anonymous, Authenticated("realm")))
    val projectRef      = ProjectRef(genUUID)
    val base            = Iri.absolute(s"http://example.com/base/").right.value
    val id              = Iri.absolute(s"http://example.com/$genUUID").right.value
    val resId           = Id(projectRef, id)
    val voc             = Iri.absolute(s"http://example.com/voc/").right.value
    // format: off
    implicit val project = Project(resId.value, genString(), genString(), None, base, voc, Map.empty, projectRef.id, genUUID, 1L, deprecated = false, Instant.EPOCH, caller.subject.id, Instant.EPOCH, caller.subject.id)
    // format: on
    resolverCache.get(projectRef) shouldReturn IO(List.empty[Resolver])

    def resolverFrom(json: Json) =
      json.addContext(viewCtxUri) deepMerge Json.obj("@id" -> Json.fromString(id.show))

    implicit val acls =
      AccessControlLists(/ -> resourceAcls(AccessControlList(caller.subject -> Set(View.write, View.query))))

  }

  trait EsView extends Base {

    val esView = jsonContentOf("/view/elasticview.json").removeKeys("_uuid") deepMerge Json.obj(
      "@id" -> Json.fromString(id.show))
    val types = Set[AbsoluteIri](nxv.View, nxv.ElasticSearchView)

    def resourceV(json: Json, rev: Long = 1L): ResourceV = {
      val graph = (json deepMerge Json.obj("@id" -> Json.fromString(id.asString)))
        .replaceContext(viewCtx)
        .asGraph(resId.value)
        .right
        .value

      val resourceV =
        ResourceF.simpleV(resId, Value(json, viewCtx.contextValue, graph), rev, schema = viewRef, types = types)
      resourceV.copy(
        value = resourceV.value.copy(graph = RootedGraph(resId.value, graph.triples ++ resourceV.metadata())))
    }

  }

  trait EsViewMocked extends EsView {
    val mapping = esView.hcursor.get[String]("mapping").flatMap(parse).right.value

    esClient.updateMapping(any[String], mEq("doc"), mEq(mapping)) shouldReturn IO(true)
    aclsCache.list shouldReturn IO.pure(acls)
    esClient.createIndex(any[String], any[Json]) shouldReturn IO(true)

  }

  "A Views bundle" when {

    "performing create operations" should {

      "prevent to create a view that does not validate against the view schema" in new Base {
        val invalid = List.range(1, 3).map(i => jsonContentOf(s"/view/aggelasticviewwrong$i.json"))
        forAll(invalid) { j =>
          val json = resolverFrom(j)
          views.create(json).value.rejected[InvalidResource]
        }
      }

      "create a view" in {
        val valid =
          List(jsonContentOf("/view/aggelasticviewrefs.json"), jsonContentOf("/view/aggelasticview.json"))
        val tpes = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticSearchView)
        forAll(valid) { j =>
          new Base {
            val json     = resolverFrom(j)
            val result   = views.create(json).value.accepted
            val expected = ResourceF.simpleF(resId, json, schema = viewRef, types = tpes)
            result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
          }
        }
      }

      "prevent creating a ElasticSearchView when ElasticSearch client throws" in new EsView {
        esClient.createIndex(any[String], any[Json]) shouldReturn IO.raiseError(
          ElasticServerError(StatusCodes.BadRequest, "Error on creation..."))
        whenReady(views.create(resId, esView).value.unsafeToFuture().failed)(_ shouldBe a[ElasticServerError])
      }

      "prevent creating a ElasticSearchView when ElasticSearch client fails while verifying mappings" in new EsView {
        esClient.createIndex(any[String], any[Json]) shouldReturn IO(true)
        esClient.updateMapping(any[String], mEq("doc"), any[Json]) shouldReturn
          IO.raiseError(ElasticServerError(StatusCodes.BadRequest, "Error on mappings..."))

        whenReady(views.create(resId, esView).value.unsafeToFuture().failed)(_ shouldBe a[ElasticServerError])
      }

      "prevent creating a ElasticSearchView when ElasticSearch index does not exist" in new EsView {
        esClient.createIndex(any[String], any[Json]) shouldReturn IO(true)
        esClient.updateMapping(any[String], mEq("doc"), any[Json]) shouldReturn IO(false)

        whenReady(views.create(resId, esView).value.unsafeToFuture().failed)(_ shouldBe a[KgError.InternalError])
      }

      "prevent creating an AggregatedElasticSearchView when project not found in the cache" in new Base {
        val label1 = ProjectLabel("account2", "project1")
        val label2 = ProjectLabel("account2", "project2")
        projectCache.getProjectRefs(Set(label1, label2)) shouldReturn IO(Map(label1 -> None, label2 -> None))
        val json = resolverFrom(jsonContentOf("/view/aggelasticview-2.json"))
        views.create(json).value.rejected[ProjectsNotFound] shouldEqual ProjectsNotFound(Set(label1, label2))
      }

      "prevent creating an AggregatedElasticSearchView when view not found in the cache" in new Base {
        val label1 = ProjectLabel("account2", "project1")
        val label2 = ProjectLabel("account2", "project2")
        val ref1   = ProjectRef(genUUID)
        val ref2   = ProjectRef(genUUID)
        projectCache.getProjectRefs(Set(label1, label2)) shouldReturn IO(
          Map(label1 -> Some(ref1), label2 -> Some(ref2)))
        viewCache.get(ref1) shouldReturn IO(Set.empty[View])
        viewCache.get(ref2) shouldReturn IO(Set.empty[View])

        val json = resolverFrom(jsonContentOf("/view/aggelasticview-2.json"))
        views.create(json).value.rejected[NotFound] shouldEqual NotFound(url"http://example.com/id4".value.ref)
      }

      "prevent creating a view with the id passed on the call not matching the @id on the payload" in new EsViewMocked {
        val json = esView deepMerge Json.obj("@id" -> Json.fromString(genIri.asString))
        views.create(resId, json).value.rejected[IncorrectId] shouldEqual IncorrectId(resId.ref)
      }
    }

    "performing update operations" should {

      "update a view" in new EsViewMocked {
        val viewUpdated = esView deepMerge Json.obj("includeMetadata" -> Json.fromBoolean(true))
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        val result   = views.update(resId, 1L, viewUpdated).value.accepted
        val expected = ResourceF.simpleF(resId, viewUpdated, 2L, schema = viewRef, types = types)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent to update a view that does not exists" in new EsViewMocked {
        views.update(resId, 1L, esView).value.rejected[NotFound] shouldEqual
          NotFound(resId.ref, Some(1L))
      }
    }

    "performing deprecate operations" should {

      "deprecate a view" in new EsViewMocked {
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        val result   = views.deprecate(resId, 1L).value.accepted
        val expected = ResourceF.simpleF(resId, esView, 2L, schema = viewRef, types = types, deprecated = true)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent deprecating a view already deprecated" in new EsViewMocked {
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        views.deprecate(resId, 1L).value.accepted shouldBe a[Resource]
        views.deprecate(resId, 2L).value.rejected[ResourceIsDeprecated] shouldBe a[ResourceIsDeprecated]
      }
    }

    "performing read operations" should {

      def uuid(resource: ResourceV) = resource.value.source.hcursor.get[String]("_uuid").right.value

      "return a view" in new EsViewMocked {
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        val result = views.fetch(resId).value.accepted
        val expected = resourceV(
          esView deepMerge Json.obj("includeMetadata" -> Json.fromBoolean(false),
                                    "_uuid"           -> Json.fromString(uuid(result))))
        result.value.source.removeKeys("@context") should equalIgnoreArrayOrder(expected.value.source)
        result.value.ctx shouldEqual expected.value.ctx
        result.value.graph shouldEqual expected.value.graph
        result shouldEqual expected.copy(value = result.value)
      }

      "return the requested view on a specific revision" in new EsViewMocked {
        val viewUpdated = esView deepMerge Json.obj("includeMetadata" -> Json.fromBoolean(true))
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        views.update(resId, 1L, viewUpdated).value.accepted shouldBe a[Resource]
        val resultLatest = views.fetch(resId, 2L).value.accepted
        val expectedLatest =
          resourceV(viewUpdated deepMerge Json.obj("_uuid" -> Json.fromString(uuid(resultLatest))), 2L)
        resultLatest.value.source.removeKeys("@context") should equalIgnoreArrayOrder(expectedLatest.value.source)
        resultLatest.value.ctx shouldEqual expectedLatest.value.ctx
        resultLatest.value.graph shouldEqual expectedLatest.value.graph
        resultLatest shouldEqual expectedLatest.copy(value = resultLatest.value)

        views.fetch(resId, 2L).value.accepted shouldEqual
          views.fetch(resId).value.accepted

        val result = views.fetch(resId, 1L).value.accepted
        val expected = resourceV(
          esView deepMerge Json.obj("includeMetadata" -> Json.fromBoolean(false),
                                    "_uuid"           -> Json.fromString(uuid(result))))
        result.value.source.removeKeys("@context") should equalIgnoreArrayOrder(expected.value.source)
        result.value.ctx shouldEqual expected.value.ctx
        result.value.graph shouldEqual expected.value.graph
        result shouldEqual expected.copy(value = result.value)
      }
    }
  }
}
