package ch.epfl.bluebrain.nexus.kg.resources

import java.nio.file.Paths
import java.time.{Clock, Instant, ZoneId}

import cats.data.EitherT
import cats.syntax.show._
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.iam.client.types.{AuthToken, Identity}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resolve.{ProjectResolution, Resolver, StaticResolution}
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.State.Initial
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.{BinaryDescription, Digest, StoredSummary}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import com.typesafe.config.ConfigFactory
import io.circe.Json
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{reset, when}
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class ResourcesSpec
    extends WordSpecLike
    with MockitoSugar
    with Matchers
    with OptionValues
    with EitherValues
    with Randomness
    with BeforeAndAfter
    with test.Resources
    with TestHelper
    with Inspectors {

  private implicit val appConfig    = new Settings(ConfigFactory.parseResources("app.conf").resolve()).appConfig
  private implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private val agg                   = MemoryAggregate("resources")(Initial, Repo.next, Repo.eval).toF[CId]
  private implicit val repo         = Repo(agg, clock)
  private implicit val store        = mock[AttachmentStore[CId, String, String]]
  private val cache                 = mock[DistributedCache[CId]]
  private val adminClient           = mock[AdminClient[CId]]
  when(cache.resolvers(ProjectRef(anyString()))).thenReturn(Set.empty[Resolver])
  private implicit val saToken = Some(AuthToken("service-account-token"))
  private implicit val resolution =
    new ProjectResolution[CId](cache, StaticResolution(AppConfig.iriResolution), adminClient)
  private val resources: Resources[CId] = Resources[CId]

  private def randomIri() = Iri.absolute(s"http://example.com/$uuid").right.value

  before {
    reset(store)
  }

  trait Base {
    implicit val identity: Identity = Anonymous
    val projectRef                  = ProjectRef(uuid)
    val base                        = Iri.absolute(s"http://example.com/base").right.value
    val id                          = Iri.absolute(s"http://example.com/$uuid").right.value
    val resId                       = Id(projectRef, id)
  }

  trait ResolverResource extends Base {
    def resolverFrom(json: Json) =
      json appendContextOf resolverCtx deepMerge Json.obj("@id" -> Json.fromString(id.show))

    val schema   = Latest(resolverSchemaUri)
    val resolver = resolverFrom(jsonContentOf("/resolve/cross-project.json"))

    val resolverUpdated = resolverFrom(jsonContentOf("/resolve/cross-project-updated.json"))
    val types           = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)
  }

  trait ViewResource extends Base {
    val schema = Latest(viewSchemaUri)
    val view = jsonContentOf("/view/elasticview.json") appendContextOf viewCtx deepMerge Json.obj(
      "@id" -> Json.fromString(id.show))
    val types = Set[AbsoluteIri](nxv.View, nxv.ElasticView, nxv.Alpha)
  }

  trait ResolverSchema extends Base {
    val schema   = Latest(shaclSchemaUri)
    val resolver = jsonContentOf("/schemas/resolver.json") deepMerge Json.obj("@id" -> Json.fromString(id.show))
    val types    = Set[AbsoluteIri](nxv.Schema)
  }

  trait Attachment extends ResolverResource {
    val desc       = BinaryDescription("name", "text/plain")
    val source     = "some text"
    val relative   = Paths.get("./other")
    val attributes = desc.process(StoredSummary(relative, 20L, Digest("MD5", "1234")))
    when(store.save(resId, desc, source)).thenReturn(EitherT.rightT[CId, Rejection](attributes))
    when(store.save(resId, desc, source)).thenReturn(EitherT.rightT[CId, Rejection](attributes))
  }

  "A Resources bundle" when {

    "performing create operations" should {

      "create a new resource validated against empty schema (resource schema) with a payload only containing @id and @context" in new ResolverResource {
        private val schemaRef = Ref(resourceSchemaUri)
        private val genId     = randomIri()
        private val genRes    = Id(projectRef, genId)
        private val json =
          Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)),
                   "@id"      -> Json.fromString(genId.show))
        resources.create(projectRef, base, schemaRef, json).value.right.value shouldEqual
          ResourceF.simpleF(genRes, json, schema = schemaRef)
      }

      "create a new resource validated against empty schema (resource schema) with a payload only containing @context" in new ResolverResource {
        private val schemaRef = Ref(resourceSchemaUri)
        private val json      = Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)))
        private val resource  = resources.create(projectRef, base, schemaRef, json).value.right.value
        resource shouldEqual ResourceF.simpleF(Id(projectRef, resource.id.value), json, schema = schemaRef)
      }

      "create a new resource validated against empty schema (resource schema) with the id passed on the call and the payload only containing @context" in new ResolverResource {
        private val schemaRef = Ref(resourceSchemaUri)
        private val json      = Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)))
        private val resource  = resources.createWithId(resId, schemaRef, json).value.right.value
        resource shouldEqual ResourceF.simpleF(Id(projectRef, resource.id.value), json, schema = schemaRef)
      }

      "create a new resource validated against empty schema (resource schema) with the id passed on the call and the payload only containing @context and @id" in new ResolverResource {
        private val schemaRef = Ref(resourceSchemaUri)
        private val json =
          Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)),
                   "@id"      -> Json.fromString(resId.value.asString))
        private val resource = resources.createWithId(resId, schemaRef, json).value.right.value
        resource shouldEqual ResourceF.simpleF(Id(projectRef, resource.id.value), json, schema = schemaRef)
      }

      "prevent to create a new resource validated against empty schema (resource schema) with the id passed on the call not matching the @id on the payload" in new ResolverResource {
        private val schemaRef = Ref(resourceSchemaUri)
        private val genId     = randomIri()
        private val json = Json.obj("@context" -> Json.obj("nxv" -> Json.fromString(nxv.base.toString)),
                                    "@id" -> Json.fromString(genId.show))
        resources.createWithId(resId, schemaRef, json).value.left.value shouldEqual IncorrectId(resId.ref)
      }

      "create a new resource validated against the resolver schema without passing the id on the call (but provided on the Json)" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldEqual
          ResourceF.simpleF(resId, resolver, schema = schema, types = types)
      }

      "create a new resource validated against the view schema without passing the id on the call (but provided on the Json)" in new ViewResource {
        resources.create(projectRef, base, schema, view).value.right.value shouldEqual
          ResourceF.simpleF(resId, view, schema = schema, types = types)
      }

      "create a new resource validated against the resolver schema without passing the id on the call (neither on the Json)" in new ResolverResource {
        private val resolverNoId = resolver removeKeys "@id"
        private val result       = resources.create(projectRef, base, schema, resolverNoId).value.right.value
        private val generatedId  = result.id.copy(parent = projectRef)
        result.id.value.show should startWith(base.show)
        result shouldEqual ResourceF.simpleF(generatedId, resolverNoId, schema = schema, types = types)
      }

      "create a new resource validated against the resolver schema passing the id on the call (the provided on the Json)" in new ResolverResource {
        resources.createWithId(resId, schema, resolver).value.right.value shouldEqual
          ResourceF.simpleF(resId, resolver, schema = schema, types = types)
      }

      "create resources that validate against resolver schema" in {
        val valid = List(
          nxv.CrossProject.value -> jsonContentOf("/resolve/cross-project.json"),
          nxv.CrossProject.value -> jsonContentOf("/resolve/cross-project2.json"),
          nxv.InAccount.value    -> jsonContentOf("/resolve/in-account.json"),
          nxv.InProject.value    -> jsonContentOf("/resolve/in-project.json")
        )
        forAll(valid) {
          case (tpe, j) =>
            new ResolverResource {
              val json = resolverFrom(j)
              resources.create(projectRef, base, schema, json).value.right.value shouldEqual
                ResourceF.simpleF(resId, json, schema = schema, types = Set(tpe, nxv.Resolver.value))
            }
        }
      }

      "prevent to create a resource that does not validate" in new ResolverResource {
        val invalid = List.range(1, 3).map(i => jsonContentOf(s"/resolve/cross-project-wrong-$i.json"))
        forAll(invalid) { j =>
          val json   = resolverFrom(j)
          val report = resources.create(projectRef, base, schema, json).value.left.value
          report shouldBe a[InvalidResource]
        }
      }

      "prevent to create a resource with non existing schema" in new ResolverResource {
        private val refSchema = Ref(randomIri())
        resources.create(projectRef, base, refSchema, resolver).value.left.value shouldEqual NotFound(refSchema)
      }

      "prevent to create a resource with wrong context value" in new ResolverResource {
        private val json = resolver deepMerge Json.obj(
          "@context" -> Json.arr(Json.fromString(resolverCtxUri.show), Json.fromInt(3)))
        resources.create(projectRef, base, schema, json).value.left.value shouldEqual IllegalContextValue(List.empty)
      }

      "prevent to create a resource with wrong context that cannot be resolved" in new ResolverResource {
        private val notFoundIri = randomIri()
        private val json        = resolver removeKeys "@context" addContext resolverCtxUri addContext notFoundIri
        resources.create(projectRef, base, schema, json).value.left.value shouldEqual NotFound(Ref(notFoundIri))
      }

      "prevent creating a schema which doesn't have nxv:Schema type" in new ResolverSchema {
        private val json = resolver deepMerge Json.obj("@type" -> Json.fromString("nxv:Resource"))
        resources.createWithId(resId, schema, json).value.left.value shouldEqual
          IncorrectTypes(resId.ref, Set(nxv.Resource.value))
      }

      "prevent creating a schema with wrong imports" in new ResolverSchema {
        private val json = resolver deepMerge Json.obj("imports" -> Json.fromString("http://example.com/some"))
        resources.createWithId(resId, schema, json).value.left.value shouldEqual
          NotFound(Ref(url"http://example.com/some".value))
      }

      "create a new schema passing the id on the call (the provided on the Json)" in new ResolverSchema {
        resources.createWithId(resId, schema, resolver).value.right.value shouldEqual
          ResourceF.simpleF(resId, resolver, types = types, schema = schema)
      }

      "create a new schema without passing the id on the call (but provided on the Json)" in new ResolverSchema {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldEqual
          ResourceF.simpleF(resId, resolver, types = types, schema = schema)
      }

      /**
        * Known issue when the Json doesn't contain an @id and the call to `graph.primaryNode` assigns a "wrong"
        * primaryNode because of "@reverse" JSON-LD key present on the @context
        */
      "create a new schema without passing the id on the call (neither on the Json)" ignore new ResolverSchema {
        private val resolverNoId = resolver removeKeys "@id"
        private val result       = resources.create(projectRef, base, schema, resolverNoId).value.right.value
        private val generatedId  = result.id.copy(parent = projectRef)
        result.id.value.show should startWith(base.show)
        result shouldEqual ResourceF.simpleF(generatedId, resolverNoId, schema = schema, types = types)
      }
    }

    "performing update operations" should {

      "update a resource (resolver)" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.update(resId, 1L, None, resolverUpdated).value.right.value shouldEqual
          ResourceF.simpleF(resId, resolverUpdated, 2L, schema = schema, types = types)
      }

      "update a resource (resolver) when the provided schema matches the created schema" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.update(resId, 1L, Some(schema), resolverUpdated).value.right.value shouldEqual
          ResourceF.simpleF(resId, resolverUpdated, 2L, schema = schema, types = types)
      }

      "prevent to update a resource (resolver) when the provided schema does not match the created schema" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        private val schemaRef = Ref(randomIri())
        resources.update(resId, 1L, Some(schemaRef), resolverUpdated).value.left.value shouldEqual NotFound(schemaRef)
      }

      "prevent to update a resource (resolver) that does not exists" in new ResolverResource {
        resources.update(resId, 1L, Some(schema), resolverUpdated).value.left.value shouldEqual NotFound(resId.ref)
      }
    }

    "performing tag operations" should {
      val tag = Json.obj("tag" -> Json.fromString("some"), "rev" -> Json.fromLong(1L))

      "tag a resource" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.update(resId, 1L, None, resolverUpdated).value.right.value shouldBe a[Resource]
        resources.tag(resId, 2L, None, tag).value.right.value shouldEqual
          ResourceF
            .simpleF(resId, resolverUpdated, 3L, schema = schema, types = types)
            .copy(tags = Map("some" -> 1L))
      }

      "prevent tagging a resource with incorrect payload" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.update(resId, 1L, None, resolverUpdated).value.right.value shouldBe a[Resource]
        private val invalidPayload: Json = Json.obj("a" -> Json.fromString("b"))
        resources.tag(resId, 2L, None, invalidPayload).value.left.value shouldBe a[InvalidPayload]
      }

      "prevent tagging a resource when the provided schema does not match the created schema" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        private val schemaRef = Ref(randomIri())
        resources.tag(resId, 1L, Some(schemaRef), tag).value.left.value shouldEqual NotFound(schemaRef)
      }
    }

    "performing deprecate operations" should {
      "deprecate a resource" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.deprecate(resId, 1L, None).value.right.value shouldEqual
          ResourceF.simpleF(resId, resolver, 2L, schema = schema, types = types, deprecated = true)
      }

      "prevent deprecating a resource when the provided schema does not match the created schema" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        private val schemaRef = Ref(randomIri())
        resources.deprecate(resId, 1L, Some(schemaRef)).value.left.value shouldEqual NotFound(schemaRef)
      }
    }

    "performing add attachment operations" should {
      "add an attachment to a resource" in new Attachment {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.attach(resId, 1L, None, desc, source).value.right.value shouldEqual
          ResourceF.simpleF(resId, resolver, 2L, schema = schema, types = types).copy(attachments = Set(attributes))
      }

      "prevent adding an attachment to a resource when the provided schema does not match the created schema" in new Attachment {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        private val schemaRef = Ref(randomIri())
        resources.attach(resId, 1L, Some(schemaRef), desc, source).value.left.value shouldEqual NotFound(schemaRef)
      }
    }

    "performing remove attachments operations" should {
      "remove an attachment from a resource" in new Attachment {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.attach(resId, 1L, None, desc, source).value.right.value shouldBe a[Resource]
        resources.unattach(resId, 2L, None, "name").value.right.value shouldBe a[Resource]
      }

      "prevent removing an attachment from resource when the provided schema does not match the created schema" in new Attachment {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.attach(resId, 1L, None, desc, source).value.right.value shouldBe a[Resource]
        private val schemaRef = Ref(randomIri())
        resources.unattach(resId, 2L, Some(schemaRef), "name").value.left.value shouldEqual NotFound(schemaRef)
      }
    }

    "performing fetch operations" should {

      "return a resource" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.fetch(resId, None).value.value shouldEqual
          ResourceF.simpleF(resId, resolver, schema = schema, types = types)
      }

      "return a specific revision of the resource" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.fetch(resId, None).value.value shouldEqual
          ResourceF.simpleF(resId, resolver, schema = schema, types = types)
      }

      "return the requested resource on a specific revision" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.update(resId, 1L, None, resolverUpdated).value.right.value shouldBe a[Resource]
        resources.fetch(resId, 2L, None).value.value shouldEqual
          ResourceF.simpleF(resId, resolverUpdated, 2L, schema = schema, types = types)
        resources.fetch(resId, 1L, None).value.value shouldEqual
          ResourceF.simpleF(resId, resolver, schema = schema, types = types)
        resources.fetch(resId, 2L, None).value.value shouldEqual resources.fetch(resId, None).value.value
      }

      "return the requested resource on a specific tag" in new ResolverResource {
        private val tag = Json.obj("tag" -> Json.fromString("some"), "rev" -> Json.fromLong(2L))
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        resources.update(resId, 1L, None, resolverUpdated).value.right.value shouldBe a[Resource]
        resources.tag(resId, 2L, None, tag).value.right.value shouldBe a[Resource]
        resources.fetch(resId, "some", None).value.value shouldEqual
          ResourceF.simpleF(resId, resolverUpdated, 2L, schema = schema, types = types)
        resources.fetch(resId, "some", None).value.value shouldEqual resources.fetch(resId, 2L, None).value.value
      }

      "return None when the provided schema does not match the created schema" in new ResolverResource {
        resources.create(projectRef, base, schema, resolver).value.right.value shouldBe a[Resource]
        private val schemaRef = Ref(randomIri())
        resources.fetch(resId, Some(schemaRef)).value shouldEqual None
      }
    }

    "performing materialize operations" should {
      "materialize a resource" in new ResolverResource {
        private val resource     = resources.create(projectRef, base, schema, resolver).value.right.value
        private val materialized = resources.materialize(resource).value.right.value
        materialized.value.source shouldEqual resolver
        materialized.value.ctx shouldEqual resolverCtx.contextValue
      }
    }
  }
}
