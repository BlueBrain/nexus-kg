package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}

import akka.stream.ActorMaterializer
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, Randomness}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.cache.{AclsCache, ProjectCache, ResolverCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resolve.{Materializer, ProjectResolution, Resolver, StaticResolution}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Iri, RootedGraph}
import io.circe.Json
import org.mockito.ArgumentMatchers.any
import org.mockito.IdiomaticMockito
import org.scalatest._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

//noinspection TypeAnnotation
class ResourcesSpec
    extends ActorSystemFixture("ResourcesSpec", true)
    with IOEitherValues
    with IOOptionValues
    with WordSpecLike
    with IdiomaticMockito
    with Matchers
    with OptionValues
    with EitherValues
    with Randomness
    with test.Resources
    with TestHelper
    with Inspectors {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3 second, 15 milliseconds)

  private implicit val appConfig              = Settings(system).appConfig
  private implicit val clock: Clock           = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
  private implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

  private implicit val repo = Repo[IO].ioValue
  private val projectCache  = mock[ProjectCache[IO]]
  private val resolverCache = mock[ResolverCache[IO]]
  private val aclsCache     = mock[AclsCache[IO]]
  resolverCache.get(any[ProjectRef]) shouldReturn IO.pure(List.empty[Resolver])
  aclsCache.list shouldReturn IO.pure(AccessControlLists.empty)

  private implicit val resolution =
    new ProjectResolution[IO](resolverCache, projectCache, StaticResolution(AppConfig.iriResolution), aclsCache)
  private implicit val materializer    = new Materializer(repo, resolution)
  private val resources: Resources[IO] = Resources[IO]

  trait Base {
    implicit val subject: Subject = Anonymous
    val projectRef                = ProjectRef(genUUID)
    val base                      = Iri.absolute(s"http://example.com/base/").right.value
    val id                        = Iri.absolute(s"http://example.com/$genUUID").right.value
    val resId                     = Id(projectRef, id)
    val voc                       = Iri.absolute(s"http://example.com/voc/").right.value
    implicit val project = Project(resId.value,
                                   "proj",
                                   "org",
                                   None,
                                   base,
                                   voc,
                                   Map.empty,
                                   projectRef.id,
                                   genUUID,
                                   1L,
                                   deprecated = false,
                                   Instant.EPOCH,
                                   subject.id,
                                   Instant.EPOCH,
                                   subject.id)
    val schemaRef = Ref(unconstrainedSchemaUri)

    def resourceV(json: Json, rev: Long = 1L): ResourceV = {
      val defaultCtxValue = Json.obj("@base" -> Json.fromString("http://example.com/base/"),
                                     "@vocab" -> Json.fromString("http://example.com/voc/"))
      val graph = (json deepMerge Json.obj("@context" -> defaultCtxValue, "@id" -> Json.fromString(id.asString)))
        .asGraph(resId.value)
        .right
        .value
      val resourceV = ResourceF.simpleV(resId, Value(json, defaultCtxValue, graph), rev, schema = schemaRef)
      resourceV.copy(
        value = resourceV.value.copy(graph = RootedGraph(resId.value, graph.triples ++ resourceV.metadata())))
    }

  }

  "A Resources bundle" when {

    "performing create operations" should {

      "create a new resource validated against empty schema (resource schema) with a payload only containing @id and @context" in new Base {
        val genId  = genIri
        val genRes = Id(projectRef, genId)
        val json =
          Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)),
                   "@id"      -> Json.fromString(genId.show))
        resources.create(base, schemaRef, json).value.accepted shouldEqual
          ResourceF.simpleF(genRes, json, schema = schemaRef)
      }

      "create a new resource validated against empty schema (resource schema) with a payload only containing @context" in new Base {
        val json     = Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)))
        val resource = resources.create(base, schemaRef, json).value.accepted
        resource shouldEqual ResourceF.simpleF(Id(projectRef, resource.id.value), json, schema = schemaRef)
      }

      "create a new resource validated against empty schema (resource schema) with the id passed on the call and the payload only containing @context" in new Base {
        val json     = Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)))
        val resource = resources.create(resId, schemaRef, json).value.accepted
        resource shouldEqual ResourceF.simpleF(Id(projectRef, resource.id.value), json, schema = schemaRef)
      }

      "create a new resource validated against empty schema (resource schema) with the id passed on the call and the payload only containing @context and @id" in new Base {
        val json = Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)),
                            "@id" -> Json.fromString(resId.value.asString))
        val resource = resources.create(resId, schemaRef, json).value.accepted
        resource shouldEqual ResourceF.simpleF(Id(projectRef, resource.id.value), json, schema = schemaRef)
      }

      "prevent to create a new resource validated against empty schema (resource schema) with the id passed on the call not matching the @id on the payload" in new Base {
        val genId = genIri
        val json = Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)),
                            "@id" -> Json.fromString(genId.show))
        resources.create(resId, schemaRef, json).value.rejected[IncorrectId] shouldEqual IncorrectId(resId.ref)
      }

      "prevent to create a resource with non existing schema" in new Base {
        val refSchema = Ref(genIri)
        resources.create(base, refSchema, Json.obj()).value.rejected[NotFound] shouldEqual NotFound(refSchema)
      }

      "prevent to create a resource with wrong context value" in new Base {
        val json = Json.obj("@context" -> Json.arr(Json.fromString(resolverCtxUri.show), Json.fromInt(3)))
        resources.create(base, schemaRef, json).value.rejected[IllegalContextValue] shouldEqual IllegalContextValue(
          List.empty)
      }

      "prevent to create a resource with wrong context that cannot be resolved" in new Base {
        val notFoundIri = genIri
        val json        = Json.obj() addContext resolverCtxUri addContext notFoundIri
        resources.create(base, schemaRef, json).value.rejected[NotFound] shouldEqual NotFound(Ref(notFoundIri))
      }

    }

    "performing update operations" should {

      "update a resource" in new Base {
        val json        = Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)))
        val jsonUpdated = Json.obj("one"      -> Json.fromString("two"))
        resources.create(resId, schemaRef, json).value.accepted shouldBe a[Resource]

        resources.update(resId, 1L, schemaRef, jsonUpdated).value.accepted shouldEqual
          ResourceF.simpleF(resId, jsonUpdated, 2L, schema = schemaRef)
      }

      "prevent to update a resource when the provided schema does not match the created schema" in new Base {
        val json = Json.obj("one" -> Json.fromString("two"))
        resources.create(resId, unconstrainedRef, json).value.accepted shouldBe a[Resource]
        val otherSchema = Ref(genIri)
        resources.update(resId, 1L, otherSchema, json).value.rejected[NotFound] shouldEqual
          NotFound(resId.ref, Some(1L))
      }

      "prevent to update a resource  that does not exists" in new Base {
        resources.update(resId, 1L, unconstrainedRef, Json.obj()).value.rejected[NotFound] shouldEqual
          NotFound(resId.ref, Some(1L))
      }
    }

    "performing deprecate operations" should {
      val json = Json.obj("one" -> Json.fromString("two"))

      "deprecate a resource" in new Base {
        resources.create(resId, schemaRef, json).value.accepted shouldBe a[Resource]
        resources.deprecate(resId, 1L, schemaRef).value.accepted shouldEqual
          ResourceF.simpleF(resId, json, 2L, schema = schemaRef, deprecated = true)
      }

      "prevent deprecating a resource when the provided schema does not match the created schema" in new Base {
        resources.create(resId, schemaRef, json).value.accepted shouldBe a[Resource]
        val otherSchema = Ref(genIri)
        resources.deprecate(resId, 1L, otherSchema).value.rejected[NotFound] shouldEqual NotFound(resId.ref, Some(1L))
      }
    }

    "performing read operations" should {
      val json        = Json.obj("one" -> Json.fromString("two"))
      val jsonUpdated = Json.obj("one" -> Json.fromString("three"))

      "return a resource" in new Base {

        resources.create(resId, schemaRef, json).value.accepted shouldBe a[Resource]
        resources.fetch(resId, schemaRef).value.accepted shouldEqual resourceV(json)
      }

      "return the requested resource on a specific revision" in new Base {
        resources.create(resId, schemaRef, json).value.accepted shouldBe a[Resource]
        resources.update(resId, 1L, schemaRef, jsonUpdated).value.accepted shouldBe a[Resource]
        resources.fetch(resId, 2L, schemaRef).value.accepted shouldEqual resourceV(jsonUpdated, 2L)
        resources.fetch(resId, 2L, schemaRef).value.accepted shouldEqual
          resources.fetch(resId, schemaRef).value.accepted
        resources.fetch(resId, 1L, schemaRef).value.accepted shouldEqual resourceV(json, 1L)
      }

      "return NotFound when the provided schema does not match the created schema" in new Base {
        resources.create(resId, schemaRef, json).value.accepted shouldBe a[Resource]
        val otherSchema = Ref(genIri)
        resources.fetch(resId, otherSchema).value.rejected[NotFound] shouldEqual NotFound(otherSchema)
      }
    }
  }
}
