package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}
import java.util.regex.Pattern.quote

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, CirceEq, EitherValues}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.cache.{AclsCache, ProjectCache, ResolverCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resolve.{Materializer, ProjectResolution, StaticResolution}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path./
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Iri, RootedGraph}
import com.github.ghik.silencer.silent
import io.circe.Json
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalactic.Equality
import org.scalatest.{BeforeAndAfter, Inspectors, OptionValues}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try

//noinspection TypeAnnotation
class ResolversSpec
    extends ActorSystemFixture("ResolversSpec", true)
    with IOEitherValues
    with IOOptionValues
    with AnyWordSpecLike
    with IdiomaticMockito
    with Matchers
    with OptionValues
    with EitherValues
    with test.Resources
    with TestHelper
    with Inspectors
    with BeforeAndAfter
    with CirceEq {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3.second, 15.milliseconds)

  private implicit val appConfig             = Settings(system).appConfig
  private implicit val clock: Clock          = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val ctx: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]      = IO.timer(ExecutionContext.global)
  private val fullCtx                        = resolverCtx appendContextOf resourceCtx

  private implicit val repo          = Repo[IO].ioValue
  private implicit val projectCache  = mock[ProjectCache[IO]]
  private implicit val resolverCache = mock[ResolverCache[IO]]
  private implicit val aclsCache     = mock[AclsCache[IO]]
  private val resolution =
    new ProjectResolution(repo, resolverCache, projectCache, StaticResolution[IO](iriResolution), aclsCache)
  private implicit val materializer    = new Materializer[IO](resolution, projectCache)
  private val resolvers: Resolvers[IO] = Resolvers[IO]

  private val user: Identity = User("dmontero", "ldap")
  private val identities     = List(Group("bbp-ou-neuroinformatics", "ldap2"), user)

  // format: off
  val project1 = Project(genIri, genString(), genString(), None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, deprecated = false, Instant.EPOCH, genIri, Instant.EPOCH, genIri)
  val project2 = Project(genIri, genString(), genString(), None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, deprecated = false, Instant.EPOCH, genIri, Instant.EPOCH, genIri)
  // format: on
  val label1 = ProjectLabel("account1", "project1")
  val label2 = ProjectLabel("account1", "project2")
  projectCache.get(project1.ref) shouldReturn IO.pure(Some(project1))
  projectCache.get(project2.ref) shouldReturn IO.pure(Some(project2))
  projectCache.getBy(ProjectLabel("account1", "project1")) shouldReturn IO.pure(Some(project1))
  projectCache.getBy(ProjectLabel("account1", "project2")) shouldReturn IO.pure(Some(project2))
  projectCache.getLabel(project1.ref) shouldReturn IO.pure(Some(label1))
  projectCache.getLabel(project2.ref) shouldReturn IO.pure(Some(label2))
  projectCache.getProjectRefs(List(label1, label2)) shouldReturn
    IO.pure(Map(label1 -> Option(project1.ref), label2 -> Option(project2.ref)))

  aclsCache.list shouldReturn IO(AccessControlLists(/ -> resourceAcls(AccessControlList(user -> Set(read)))))

  before {
    Mockito.reset(resolverCache)
  }

  trait Base {
    implicit lazy val caller =
      Caller(Anonymous, Set(Anonymous, Group("bbp-ou-neuroinformatics", "ldap2"), User("dmontero", "ldap")))
    val projectRef = ProjectRef(genUUID)
    val base       = Iri.absolute(s"http://example.com/base/").rightValue
    val id         = Iri.absolute(s"http://example.com/$genUUID").rightValue
    val resId      = Id(projectRef, id)
    val voc        = Iri.absolute(s"http://example.com/voc/").rightValue
    // format: off
    implicit val project = Project(resId.value, "proj", "org", None, base, voc, Map.empty, projectRef.id, genUUID, 1L, deprecated = false, Instant.EPOCH, caller.subject.id, Instant.EPOCH, caller.subject.id)
    val crossResolver = CrossProjectResolver(Set(nxv.Schema), List(project1.ref, project2.ref), identities, projectRef, url"http://example.com/id".value, 1L, false, 20)
    // format: on
    resolverCache.get(projectRef) shouldReturn IO(List(InProjectResolver.default(projectRef), crossResolver))

    def updateId(json: Json) =
      json deepMerge Json.obj("@id" -> Json.fromString(id.show))
    val resolver = updateId(jsonContentOf("/resolve/cross-project.json"))
    def resolverSource(priority: Int = 50) =
      updateId(
        jsonContentOf(
          "/resolve/cross-project-source.json",
          Map(
            quote("{uuid1}")    -> project1.uuid.toString,
            quote("{uuid2}")    -> project2.uuid.toString,
            quote("{priority}") -> priority.toString,
            quote("{base}")     -> "http://localhost:8080"
          )
        )
      )
    val types = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)

    def resourceV(json: Json, rev: Long = 1L): ResourceV = {
      val graph = (json deepMerge Json.obj("@id" -> Json.fromString(id.asString)))
        .replaceContext(resolverCtx)
        .asGraph(resId.value)
        .rightValue

      val resourceV =
        ResourceF.simpleV(resId, Value(json, resolverCtx.contextValue, graph), rev, schema = resolverRef, types = types)
      resourceV.copy(
        value = resourceV.value.copy(graph = RootedGraph(resId.value, graph.triples ++ resourceV.metadata()))
      )
    }
  }

  private implicit val ordering: Ordering[Identity] = (x: Identity, y: Identity) =>
    x.id.asString compareTo y.id.asString

  @silent // the definition is not recognized as used
  private implicit val eqCrossProject: Equality[CrossProjectResolver[ProjectRef]] =
    (a: CrossProjectResolver[ProjectRef], b: Any) => {
      Try {
        val that = b.asInstanceOf[CrossProjectResolver[ProjectRef]]
        that.copy(identities = identities.sorted) == a.copy(identities = identities.sorted)
      }.getOrElse(false)
    }

  "A Resolver bundle" when {

    "performing create operations" should {

      "prevent to create a resolver that does not validate against the resolver schema" in new Base {
        val invalid = List.range(1, 2).map(i => jsonContentOf(s"/resolve/cross-project-wrong-$i.json"))
        forAll(invalid) { j =>
          val json = updateId(j)
          resolvers.create(json).value.rejected[InvalidResource]
        }
      }

      "create a InProject resolver" in new Base {
        resolverCache.put(InProjectResolver(project.ref, id, 1L, false, 10)) shouldReturn IO.pure(())
        val json   = updateId(jsonContentOf("/resolve/in-project.json"))
        val result = resolvers.create(json).value.accepted
        val expected = {
          ResourceF.simpleF(resId, json, schema = resolverRef, types = Set[AbsoluteIri](nxv.Resolver, nxv.InProject))
        }
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "create a CrossProject resolver" in new Base {
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 50)) shouldReturn IO.pure(())
        val result   = resolvers.create(resId, resolver).value.accepted
        val expected = ResourceF.simpleF(resId, resolver, schema = resolverRef, types = types)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent creating an CrossProject when project not found in the cache" in new Base {
        val label1 = ProjectLabel("account2", "project1")
        val label2 = ProjectLabel("account2", "project2")
        projectCache.getProjectRefs(List(label1, label2)) shouldReturn IO(Map(label1 -> None, label2 -> None))
        val json = resolver.removeKeys("projects") deepMerge Json.obj(
          "projects" -> Json.arr(Json.fromString("account2/project1"), Json.fromString("account2/project2"))
        )
        resolvers.create(resId, json).value.rejected[ProjectsNotFound] shouldEqual ProjectsNotFound(Set(label1, label2))
      }

      "prevent creating a CrossProject resolver when the caller does not have some of the resolver identities" in new Base {
        override implicit lazy val caller = Caller(Anonymous, Set(Anonymous))
        resolvers.create(resId, resolver).value.rejected[InvalidIdentity]
      }

      "prevent creating a resolver with the id passed on the call not matching the @id on the payload" in new Base {
        val json = resolver deepMerge Json.obj("@id" -> Json.fromString(genIri.asString))
        resolvers.create(resId, json).value.rejected[IncorrectId] shouldEqual IncorrectId(resId.ref)
      }

    }

    "performing update operations" should {

      "update a resolver" in new Base {
        val resolverUpdated = resolver deepMerge Json.obj("priority" -> Json.fromInt(34))
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 50)) shouldReturn IO.pure(())
        resolvers.create(resId, resolver).value.accepted shouldBe a[Resource]
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 34)) shouldReturn IO.pure(())
        val result   = resolvers.update(resId, 1L, resolverUpdated).value.accepted
        val expected = ResourceF.simpleF(resId, resolverUpdated, 2L, schema = resolverRef, types = types)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent to update a resolver that does not exists" in new Base {
        resolvers.update(resId, 1L, resolver).value.rejected[NotFound] shouldEqual NotFound(resId.ref)
      }
    }

    "performing deprecate operations" should {

      "deprecate a resolver" in new Base {
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 50)) shouldReturn IO.pure(())
        resolvers.create(resId, resolver).value.accepted shouldBe a[Resource]
        val result   = resolvers.deprecate(resId, 1L).value.accepted
        val expected = ResourceF.simpleF(resId, resolver, 2L, schema = resolverRef, types = types, deprecated = true)
        result.copy(value = Json.obj()) shouldEqual expected.copy(value = Json.obj())
      }

      "prevent deprecating a resolver already deprecated" in new Base {
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 50)) shouldReturn IO.pure(())
        resolvers.create(resId, resolver).value.accepted shouldBe a[Resource]
        resolvers.deprecate(resId, 1L).value.accepted shouldBe a[Resource]
        resolvers.deprecate(resId, 2L).value.rejected[ResourceIsDeprecated] shouldBe a[ResourceIsDeprecated]
      }
    }

    "performing read operations" should {

      def resolverForGraph(id: AbsoluteIri) =
        jsonContentOf("/resolve/cross-project-to-graph.json", Map(quote("{id}") -> id.asString))

      projectCache.getProjectLabels(List(project1.ref, project2.ref)) shouldReturn IO(
        Map(project1.ref -> Some(label1), project2.ref -> Some(label2))
      )

      "return a resolver" in new Base {
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 50)) shouldReturn IO.pure(())
        resolvers.create(resId, resolver).value.accepted shouldBe a[Resource]
        val result = resolvers.fetch(resId).value.accepted
        resolvers.fetchSource(resId).value.accepted should equalIgnoreArrayOrder(resolverSource())
        val expected = resourceV(resolverForGraph(resId.value))
        val json     = removeMetadata(result.value.graph).as[Json](fullCtx).rightValue.removeKeys("@context")
        json should equalIgnoreArrayOrder(resolverForGraph(resId.value))
        result.value.ctx shouldEqual expected.value.ctx
        result shouldEqual expected.copy(value = result.value)
      }

      "return the requested resolver on a specific revision" in new Base {
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 50)) shouldReturn IO.pure(())
        val resolverUpdated         = resolver deepMerge Json.obj("priority"                      -> Json.fromInt(34))
        val resolverUpdatedForGraph = resolverForGraph(resId.value) deepMerge Json.obj("priority" -> Json.fromInt(34))
        resolvers.create(resId, resolver).value.accepted shouldBe a[Resource]
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 34)) shouldReturn IO.pure(())
        resolvers.update(resId, 1L, resolverUpdated).value.accepted shouldBe a[Resource]

        val resultLatest   = resolvers.fetch(resId, 2L).value.accepted
        val expectedLatest = resourceV(resolverUpdatedForGraph, 2L)
        resultLatest.value.ctx shouldEqual expectedLatest.value.ctx
        val json1 = removeMetadata(resultLatest.value.graph).as[Json](fullCtx).rightValue.removeKeys("@context")
        json1 should equalIgnoreArrayOrder(resolverUpdatedForGraph)
        resultLatest shouldEqual expectedLatest.copy(value = resultLatest.value)

        resolvers.fetchSource(resId, 2L).value.accepted should equalIgnoreArrayOrder(resolverSource(34))

        val result   = resolvers.fetch(resId, 1L).value.accepted
        val expected = resourceV(resolverForGraph(resId.value))
        result.value.ctx shouldEqual expected.value.ctx
        val json2 = removeMetadata(result.value.graph).as[Json](fullCtx).rightValue.removeKeys("@context")
        json2 should equalIgnoreArrayOrder(resolverForGraph(resId.value))
        result shouldEqual expected.copy(value = result.value)
      }

      "return NotFound when the provided resolver does not exists" in new Base {
        resolvers.fetch(resId).value.rejected[NotFound] shouldEqual NotFound(resId.ref, schemaOpt = Some(resolverRef))
        resolvers.fetchSource(resId).value.rejected[NotFound] shouldEqual
          NotFound(resId.ref, schemaOpt = Some(resolverRef))
      }
    }

    "performing resolve operations" should {

      "return resolved resource" in new Base {
        val defaultCtx = Json.obj(
          "@context" -> Json.obj(
            "@base"  -> Json.fromString(project1.base.asString),
            "@vocab" -> Json.fromString(project1.vocab.asString)
          )
        )
        val resourceId = genIri
        val orgRef     = OrganizationRef(project1.organizationUuid)
        resolverCache.put(crossResolver.copy(id = resId.value, priority = 50)) shouldReturn IO.pure(())
        resolvers.create(resId, resolver).value.accepted shouldBe a[Resource]
        val json = Json.obj("key" -> Json.fromString("value")) deepMerge defaultCtx
        repo.create(Id(project1.ref, resourceId), orgRef, shaclRef, Set(nxv.Schema), json).value.accepted
        val resource = repo.get(Id(project1.ref, resourceId), None).value.some
        val graph = RootedGraph(
          resourceId,
          resource.metadata()(appConfig, project1) + ((resourceId, url"${project1.vocab.asString}key", "value"): Triple)
        )
        val ctx      = defaultCtx.contextValue deepMerge resourceCtx.contextValue
        val expected = resource.map(json => Value(json, ctx, graph))
        resolvers.resolve(resourceId).value.accepted shouldEqual expected
        resolvers.resolve(resId, resourceId).value.accepted shouldEqual expected
        resolvers.resolve(resourceId, 1L).value.accepted shouldEqual expected
        resolvers.resolve(resId, resourceId, 1L).value.accepted shouldEqual expected
        resolvers.resolve(resourceId, 2L).value.rejected[NotFound]
        resolvers.resolve(resId, resourceId, 2L).value.rejected[NotFound]
      }

      "return not found" in new Base {
        val resourceId = genIri
        resolvers.resolve(resourceId).value.rejected[NotFound]
      }
    }
  }
}
