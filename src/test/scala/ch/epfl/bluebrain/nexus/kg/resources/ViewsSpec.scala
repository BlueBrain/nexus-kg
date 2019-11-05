package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.ElasticServerError
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, CirceEq}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.cache.{AclsCache, ProjectCache, ResolverCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.indexing.View.Filter
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resolve.{Materializer, ProjectResolution, Resolver, StaticResolution}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.{KgError, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Iri, RootedGraph}
import io.circe.Json
import io.circe.parser.parse
import org.mockito.matchers.MacroBasedMatchers
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito, Mockito}
import org.scalatest._
import java.util.regex.Pattern.quote

import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.storage.client.StorageClient
import monix.eval.Task

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

//noinspection TypeAnnotation
class ViewsSpec
    extends ActorSystemFixture("ViewsSpec", true)
    with IOEitherValues
    with IOOptionValues
    with WordSpecLike
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with MacroBasedMatchers
    with Matchers
    with OptionValues
    with EitherValues
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
  private implicit val sparqlClient  = mock[BlazegraphClient[IO]]
  private implicit val adminClient   = mock[AdminClient[IO]]
  private implicit val iamClient     = mock[IamClient[IO]]
  private implicit val storageClient = mock[StorageClient[IO]]
  private implicit val rsearchClient = mock[HttpClient[IO, QueryResults[Json]]]
  private implicit val taskJson      = mock[HttpClient[Task, Json]]
  private implicit val untyped       = HttpClient.untyped[Task]
  private implicit val esClient      = mock[ElasticSearchClient[IO]]
  private val aclsCache              = mock[AclsCache[IO]]
  private implicit val resolverCache = mock[ResolverCache[IO]]
  private implicit val clients       = Clients()

  private val resolution =
    new ProjectResolution(repo, resolverCache, projectCache, StaticResolution[IO](iriResolution), mock[AclsCache[IO]])
  private implicit val materializer = new Materializer[IO](resolution, projectCache)

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
    Map(label1 -> Option(project1.ref), label2 -> Option(project2.ref))
  )

  private val views: Views[IO] = Views[IO]

  before {
    Mockito.reset(resolverCache, viewCache)
  }

  trait Base {

    viewCache.get(project1.ref) shouldReturn IO.pure(
      Set[View](ElasticSearchView.default(project1.ref).copy(id = url"http://example.com/id2".value))
    )
    viewCache.get(project2.ref) shouldReturn IO.pure(
      Set[View](ElasticSearchView.default(project2.ref).copy(id = url"http://example.com/id3".value))
    )

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

    def viewFrom(json: Json) =
      json.addContext(viewCtxUri) deepMerge Json.obj("@id" -> Json.fromString(id.show))

    implicit val acls =
      AccessControlLists(/ -> resourceAcls(AccessControlList(caller.subject -> Set(View.write, View.query))))

    def matchesIgnoreId(that: View): View => Boolean = {
      case view: View.AggregateElasticSearchView[_] => view.copy(uuid = that.uuid) == that
      case view: View.AggregateSparqlView[_]        => view.copy(uuid = that.uuid) == that
      case view: ElasticSearchView                  => view.copy(uuid = that.uuid) == that
      case view: View.SparqlView                    => view.copy(uuid = that.uuid) == that
      case view: View.CompositeView                 => view.copy(uuid = that.uuid) == that
    }

  }

  trait EsView extends Base {

    val esView = jsonContentOf("/view/elasticview.json").removeKeys("_uuid") deepMerge Json.obj(
      "@id" -> Json.fromString(id.show)
    )
    def esViewSource(uuid: String, includeMeta: Boolean = false) =
      jsonContentOf(
        "/view/elasticview-source.json",
        Map(quote("{uuid}") -> uuid, quote("{includeMetadata}") -> includeMeta.toString)
      ) deepMerge Json
        .obj("@id" -> Json.fromString(id.show))
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
        value = resourceV.value.copy(graph = RootedGraph(resId.value, graph.triples ++ resourceV.metadata()))
      )
    }

  }

  trait EsViewMocked extends EsView {
    val mapping = esView.hcursor.get[String]("mapping").flatMap(parse).right.value
    // format: off
    val esViewModel = ElasticSearchView(mapping, Filter(Set(nxv.Schema.value, nxv.Resource.value), Set(nxv.withSuffix("MyType").value, nxv.withSuffix("MyType2").value), Some("one")), includeMetadata = false, sourceAsText = true, project.ref, id, UUID.randomUUID(), 1L, deprecated = false)
    // format: on
    esClient.updateMapping(any[String], eqTo(mapping)) shouldReturn IO(true)
    aclsCache.list shouldReturn IO.pure(acls)
    esClient.createIndex(any[String], any[Json]) shouldReturn IO(true)

  }

  "A Views bundle" when {

    "performing create operations" should {

      "prevent to create a view that does not validate against the view schema" in new Base {
        val invalid = List.range(1, 3).map(i => jsonContentOf(s"/view/aggelasticviewwrong$i.json"))
        forAll(invalid) { j =>
          val json = viewFrom(j)
          views.create(json).value.rejected[InvalidResource]
        }
      }

      "create a view" in {
        viewCache.put(any[View]) shouldReturn IO(())
        val valid =
          List(jsonContentOf("/view/aggelasticviewrefs.json"), jsonContentOf("/view/aggelasticview.json"))
        val tpes = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticSearchView)
        forAll(valid) { j =>
          new Base {
            val json     = viewFrom(j)
            val result   = views.create(json).value.accepted
            val expected = ResourceF.simpleF(resId, json, schema = viewRef, types = tpes)
            result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
          }
        }
      }

      "create default Elasticsearch view using payloads' uuid" in new EsView {
        val view: View = ElasticSearchView.default(projectRef)
        val defaultId  = Id(projectRef, nxv.defaultElasticSearchIndex)
        val json = view
          .as[Json](viewCtx.appendContextOf(resourceCtx))
          .right
          .value
          .removeKeys(nxv.rev.prefix, nxv.deprecated.prefix)
          .replaceContext(viewCtxUri)
        val mapping = json.hcursor.get[String]("mapping").flatMap(parse).right.value

        esClient.updateMapping(any[String], eqTo(mapping)) shouldReturn IO(true)
        aclsCache.list shouldReturn IO.pure(acls)
        esClient.createIndex(any[String], any[Json]) shouldReturn IO(true)

        viewCache.put(view) shouldReturn IO(())

        views.create(defaultId, json, extractUuid = true).value.accepted shouldEqual
          ResourceF.simpleF(defaultId, json, schema = viewRef, types = types)
      }

      "prevent creating a ElasticSearchView when ElasticSearch client throws" in new EsView {
        esClient.createIndex(any[String], any[Json]) shouldReturn IO.raiseError(
          ElasticServerError(StatusCodes.BadRequest, "Error on creation...")
        )
        whenReady(views.create(resId, esView).value.unsafeToFuture().failed)(_ shouldBe a[ElasticServerError])
      }

      "prevent creating a ElasticSearchView when ElasticSearch client fails while verifying mappings" in new EsView {
        esClient.createIndex(any[String], any[Json]) shouldReturn IO(true)
        esClient.updateMapping(any[String], any[Json]) shouldReturn
          IO.raiseError(ElasticServerError(StatusCodes.BadRequest, "Error on mappings..."))

        whenReady(views.create(resId, esView).value.unsafeToFuture().failed)(_ shouldBe a[ElasticServerError])
      }

      "prevent creating a ElasticSearchView when ElasticSearch index does not exist" in new EsView {
        esClient.createIndex(any[String], any[Json]) shouldReturn IO(true)
        esClient.updateMapping(any[String], any[Json]) shouldReturn IO(false)

        whenReady(views.create(resId, esView).value.unsafeToFuture().failed)(_ shouldBe a[KgError.InternalError])
      }

      "prevent creating an AggregatedElasticSearchView when project not found in the cache" in new Base {
        val label1 = ProjectLabel("account2", "project1")
        val label2 = ProjectLabel("account2", "project2")
        projectCache.getProjectRefs(Set(label1, label2)) shouldReturn IO(Map(label1 -> None, label2 -> None))
        val json = viewFrom(jsonContentOf("/view/aggelasticview-2.json"))
        views.create(json).value.rejected[ProjectsNotFound] shouldEqual ProjectsNotFound(Set(label1, label2))
      }

      "prevent creating an AggregatedElasticSearchView when view not found in the cache" in new Base {
        val label1 = ProjectLabel("account2", "project1")
        val label2 = ProjectLabel("account2", "project2")
        val ref1   = ProjectRef(genUUID)
        val ref2   = ProjectRef(genUUID)
        projectCache.getProjectRefs(Set(label1, label2)) shouldReturn IO(
          Map(label1 -> Some(ref1), label2 -> Some(ref2))
        )
        viewCache.get(ref1) shouldReturn IO(Set.empty[View])
        viewCache.get(ref2) shouldReturn IO(Set.empty[View])

        val json = viewFrom(jsonContentOf("/view/aggelasticview-2.json"))
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
        viewCache.put(argThat(matchesIgnoreId(esViewModel), "")) shouldReturn IO(())
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        Mockito.reset(viewCache)
        viewCache.put(argThat(matchesIgnoreId(esViewModel.copy(includeMetadata = true)), "")) shouldReturn IO(())
        val result   = views.update(resId, 1L, viewUpdated).value.accepted
        val expected = ResourceF.simpleF(resId, viewUpdated, 2L, schema = viewRef, types = types)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent to update a view that does not exists" in new EsViewMocked {
        views.update(resId, 1L, esView).value.rejected[NotFound] shouldEqual
          NotFound(resId.ref, schemaOpt = Some(viewRef))
      }
    }

    "performing deprecate operations" should {

      "deprecate a view" in new EsViewMocked {
        viewCache.put(argThat(matchesIgnoreId(esViewModel), "")) shouldReturn IO(())
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        val result   = views.deprecate(resId, 1L).value.accepted
        val expected = ResourceF.simpleF(resId, esView, 2L, schema = viewRef, types = types, deprecated = true)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent deprecating a view already deprecated" in new EsViewMocked {
        viewCache.put(argThat(matchesIgnoreId(esViewModel), "")) shouldReturn IO(())
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        views.deprecate(resId, 1L).value.accepted shouldBe a[Resource]
        views.deprecate(resId, 2L).value.rejected[ResourceIsDeprecated] shouldBe a[ResourceIsDeprecated]
      }
    }

    "performing read operations" should {

      def uuid(resource: ResourceV) = resource.value.source.hcursor.get[String]("_uuid").right.value

      "return a view" in new EsViewMocked {
        viewCache.put(argThat(matchesIgnoreId(esViewModel), "")) shouldReturn IO(())
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        val result = views.fetch(resId).value.accepted
        val expected = resourceV(
          esView deepMerge Json.obj(
            "includeMetadata"   -> Json.fromBoolean(false),
            "includeDeprecated" -> Json.fromBoolean(true),
            "_uuid"             -> Json.fromString(uuid(result))
          )
        )
        result.value.source.removeKeys("@context") should equalIgnoreArrayOrder(expected.value.source)
        result.value.ctx shouldEqual expected.value.ctx
        result.value.graph shouldEqual expected.value.graph
        result shouldEqual expected.copy(value = result.value)
        views.fetchSource(resId).value.accepted should equalIgnoreArrayOrder(esViewSource(uuid(result)))
      }

      "return the requested view on a specific revision" in new EsViewMocked {
        val viewUpdated = esView deepMerge Json.obj(
          "includeMetadata"   -> Json.fromBoolean(true),
          "includeDeprecated" -> Json.fromBoolean(true)
        )
        viewCache.put(argThat(matchesIgnoreId(esViewModel), "")) shouldReturn IO(())
        views.create(resId, esView).value.accepted shouldBe a[Resource]
        Mockito.reset(viewCache)
        viewCache.put(
          argThat(
            matchesIgnoreId(
              esViewModel.copy(filter = esViewModel.filter.copy(includeDeprecated = true), includeMetadata = true)
            ),
            ""
          )
        ) shouldReturn
          IO(())
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
        views.fetchSource(resId).value.accepted should
          equalIgnoreArrayOrder(esViewSource(uuid(result), includeMeta = true))
        val expected = resourceV(
          esView deepMerge Json.obj(
            "includeMetadata"   -> Json.fromBoolean(false),
            "includeDeprecated" -> Json.fromBoolean(true),
            "_uuid"             -> Json.fromString(uuid(result))
          )
        )
        result.value.source.removeKeys("@context") should equalIgnoreArrayOrder(expected.value.source)
        result.value.ctx shouldEqual expected.value.ctx
        result.value.graph shouldEqual expected.value.graph
        result shouldEqual expected.copy(value = result.value)
      }

      "return NotFound when the provided view does not exists" in new Base {
        views.fetch(resId).value.rejected[NotFound] shouldEqual NotFound(resId.ref, schemaOpt = Some(viewRef))
        views.fetchSource(resId).value.rejected[NotFound] shouldEqual NotFound(resId.ref, schemaOpt = Some(viewRef))
      }
    }
  }
}
