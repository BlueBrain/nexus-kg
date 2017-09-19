package ch.epfl.bluebrain.nexus.kg.service.routes

import java.net.URLEncoder
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.{Comparator, UUID}
import java.util.regex.Pattern

import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.{Keep, Sink}
import akka.util.ByteString
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidator
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceRejection.{IncorrectRevisionProvided, InstanceDoesNotExist, InstanceIsDeprecated, ShapeConstraintViolations}
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment
import ch.epfl.bluebrain.nexus.kg.core.instances.attachments.Attachment._
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, InstanceRef, Instances}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaRejection.{SchemaDoesNotExist, SchemaIsDeprecated, SchemaIsNotPublished}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaImportResolver, Schemas}
import ch.epfl.bluebrain.nexus.kg.core.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.instances.attachments.{AkkaInOutFileStream, RelativeAttachmentLocation}
import ch.epfl.bluebrain.nexus.kg.service.routes.Error._
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import com.typesafe.config.ConfigFactory
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{DoNotDiscover, Inspectors, Matchers, WordSpecLike}
import InstanceRoutesSpec._
import akka.http.scaladsl.model.Uri.Query
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.kg.indexing.pagination.Pagination
import ch.epfl.bluebrain.nexus.kg.indexing.query.QuerySettings
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutesSpec.{Result, Results}
import ch.epfl.bluebrain.nexus.kg.service.routes.SparqlFixtures.Source
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceEvent.{InstanceCreated, InstanceDeprecated}
import ch.epfl.bluebrain.nexus.kg.indexing.IndexerFixture
import ch.epfl.bluebrain.nexus.kg.indexing.filtering.FilteringSettings
import ch.epfl.bluebrain.nexus.kg.indexing.instances.{InstanceIndexer, InstanceIndexingSettings}
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import org.apache.jena.query.ResultSet

import scala.concurrent.Future
import scala.concurrent.duration._

@DoNotDiscover
class InstanceRoutesSpec(blazegraphPort: Int)
    extends IndexerFixture
    with WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with Randomness
    with Resources
    with ScalaFutures
    with Inspectors {

  trait Context extends ScalaFutures {

    override implicit val patienceConfig = PatienceConfig(3 seconds, 100 millis)

    def genSchema(): Json =
      Json.obj()


    def genJson(): Json =
      jsonContentOf("/int-value.json").deepMerge(Json.obj("value" -> Json.fromInt(genInt(Int.MaxValue))))

    val settings = new Settings(ConfigFactory.load())
    val algorithm = settings.Attachment.HashAlgorithm
    val schemaJson = jsonContentOf("/int-value-schema.json")

    val orgAgg = MemoryAggregate("orgs")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]
    val orgs = Organizations(orgAgg)
    val domAgg = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
    val doms = Domains(domAgg, orgs)
    val schAgg = MemoryAggregate("schemas")(Schemas.initial, Schemas.next, Schemas.eval).toF[Future]
    val schemas = Schemas(schAgg, doms, baseUri.toString())
    val validator = ShaclValidator[Future](SchemaImportResolver(baseUri.toString(), schemas.fetch))
    val instAgg = MemoryAggregate("instances")(Instances.initial, Instances.next, Instances.eval).toF[Future]
    implicit val fa = RelativeAttachmentLocation[Future](settings)
    val inFileProcessor = AkkaInOutFileStream(settings)
    val instances = Instances(instAgg, schemas, validator, inFileProcessor)


    val orgRef = orgs.create(OrgId(genString(length = 3)), Json.obj()).futureValue
    val domRef = doms.create(DomainId(orgRef.id, genString(length = 5)), genString(length = 8)).futureValue
    val schemaId = SchemaId(domRef.id, genString(length = 8), genVersion())

    val unpublished = schemas.create(schemaId, schemaJson).futureValue
    val _ = schemas.publish(schemaId, unpublished.rev).futureValue

    private val indexSettings@InstanceIndexingSettings(index, instanceBase, instanceBaseNs, nexusVocBase) =
      InstanceIndexingSettings(
        genString(length = 6),
        baseUri,
        s"$baseUri/data/graphs",
        s"$baseUri/voc/nexus/core")

    val querySettings = QuerySettings(Pagination(0L, 20), index, nexusVocBase)
    implicit val filteringSettings = FilteringSettings(nexusVocBase, nexusVocBase)
    val baseUUID = UUID.randomUUID().toString.toLowerCase().dropRight(2)

    private implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient
    private implicit val rs: HttpClient[Future, ResultSet] = HttpClient.withAkkaUnmarshaller[ResultSet]
    val client = SparqlClient[Future](s"http://$localhost:$blazegraphPort/blazegraph")
    val instanceIndexer = InstanceIndexer(client, indexSettings)

    val route = InstanceRoutes(instances, client, querySettings, baseUri).routes
    val value = genJson()
    val instanceRef = Post(s"/data/${schemaId.show}", value) ~> route ~> check {
      status shouldEqual StatusCodes.Created
      val json = responseAs[Json]
      val instanceId = InstanceId(toCompact(json.hcursor.get[String]("@id").toOption.get)).get
      InstanceRef(instanceId, json.hcursor.get[Long]("rev").toOption.get)
    }

    def deprecateInstance(ref: InstanceRef) =
      Delete(s"/data/${ref.id.show}?rev=${ref.rev}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual instanceRefAsJson(InstanceRef(ref.id, 2L))
      }

    def digestSink: Sink[ByteString, Future[MessageDigest]] = {
      val initDigest = MessageDigest.getInstance(algorithm)
      Sink.fold[MessageDigest, ByteString](initDigest)((digest, currentBytes) => {
        digest.update(currentBytes.asByteBuffer)
        digest
      })
    }

    def multipartEntityAndFileSize(filename: String): (Multipart.FormData, Long) = {
      val path = Paths.get(getClass.getResource(s"/$filename").toURI)
      Multipart.FormData(Multipart.FormData.BodyPart(
        "file",
        HttpEntity.fromPath(ContentTypes.`text/csv(UTF-8)`, path),
        Map("filename" -> filename))) -> Files.size(path)
    }

    def deleteAttachments() =
      Files.walk(settings.Attachment.VolumePath)
        .sorted(Comparator.reverseOrder())
        .forEach(p => Files.delete(p))

    def indexInstances(): Unit = {
      client.createIndex(index, properties).futureValue

      // index 5 instances with same schemaId
      (0 until 5).foreach { idx =>
        val id = InstanceId(schemaId.copy(version = Version(idx, 0, 0)), s"${baseUUID}0$idx")
        instanceIndexer(InstanceCreated(id, 1L, value)).futureValue
      }

      // index 5 instances with same schemaId but deprecated
      (5 until 10).foreach { idx =>
        val id = InstanceId(schemaId.copy(version = Version(idx, 0, 0)), s"${baseUUID}0$idx")
        instanceIndexer(InstanceCreated(id, 1L, value)).futureValue
        instanceIndexer(InstanceDeprecated(id, 2L)).futureValue
      }
      /// index 5 instances with different orgId
      (10 until 15).foreach { idx =>
        val id = InstanceId(schemaId.copy(version = Version(idx, 0, 0), domainId = schemaId.domainId.copy(orgId = OrgId("other"))), s"${baseUUID}$idx")
        instanceIndexer(InstanceCreated(id, 1L, value)).futureValue
        instanceIndexer(InstanceDeprecated(id, 2L)).futureValue
      }
    }
  }

  "An InstanceRoutes" should {

    "create an instance" in new Context {
      instanceRef.rev shouldEqual 1L
    }

    "reject the creation of an instance when schema name does not exists" in new Context {
      Post(s"/data/${schemaId.copy(name = "some").show}", value) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[SchemaDoesNotExist.type]
      }
    }

    "reject the creation of an instance when the json data does not conform to the schema" in new Context {
      Post(s"/data/${schemaId.show}", Json.obj()) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[ShapeConstraintViolations.type]
      }
    }

    "reject the creation of an instance when schema is not publish" in new Context {
      val schemaId2 = SchemaId(domRef.id, genString(length = 8), genVersion())
      schemas.create(schemaId2, schemaJson).futureValue

      Post(s"/data/${schemaId2.show}", value) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[SchemaIsNotPublished.type]
      }
    }

    "reject the creation of an instance when schema is deprecated" in new Context {
      //Create a new schema
      val schemaId2 = SchemaId(domRef.id, genString(length = 8), genVersion())
      schemas.create(schemaId2, schemaJson).futureValue

      //Deprecate the new schema
      schemas.deprecate(schemaId2, 1L).futureValue

      Post(s"/data/${schemaId2.show}", value) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[SchemaIsDeprecated.type]
      }
    }

    "return the current instance" in new Context {
      Get(s"/data/${instanceRef.id.show}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "@id" -> Json.fromString(s"$baseUri/data/${instanceRef.id.show}"),
          "rev" -> Json.fromLong(1L),
          "deprecated" -> Json.fromBoolean(false)
        ).deepMerge(value)
      }
    }

    "return an instance at a specific revision" in new Context {
      val value2 = genJson()
      Put(s"/data/${instanceRef.id.show}?rev=${instanceRef.rev}", value2) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual instanceRefAsJson(InstanceRef(instanceRef.id, 2L))
      }

      Get(s"/data/${instanceRef.id.show}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "@id" -> Json.fromString(s"$baseUri/data/${instanceRef.id.show}"),
          "rev" -> Json.fromLong(2L),
          "deprecated" -> Json.fromBoolean(false)
        ).deepMerge(value2)
      }

        Get(s"/data/${instanceRef.id.show}?rev=1") ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual Json.obj(
            "@id" -> Json.fromString(s"$baseUri/data/${instanceRef.id.show}"),
            "rev" -> Json.fromLong(1L),
            "deprecated" -> Json.fromBoolean(false)
          ).deepMerge(value)
        }
      }

    "return list of instances from domain id with specific pagination" in new Context  {
      indexInstances()
      val specificPagination = Pagination(0L, 5)
      Get(s"/data/${orgRef.id.id}/${domRef.id.id}?from=${specificPagination.from}&size=${specificPagination.size}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val results = responseAs[Results]
        results.total shouldEqual 10L
        results.results.size shouldEqual 5
        forAll(results.results.zipWithIndex) { case(result, idx) =>
          val schema = s"$baseUri/schemas/${schemaId.copy(version = Version(idx,0,0)).show}"
          val id = s"$schema/${baseUUID}0$idx".replace("/schemas/", "/data/")
          result shouldEqual Result(id,
            Source(id,List(Link("self", id),Link("schema", schema))))
        }
        results.links should contain allElementsOf
          List(Link("next", s"$base/data/${orgRef.id.id}/${domRef.id.id}?from=5&size=5"), Link("self", s"$base/data/${orgRef.id.id}/${domRef.id.id}?from=0&size=5"))
      }
    }

    "return list of instances from schema name with deprecated and filter" in new Context  {
      indexInstances()
      val specificPagination = Pagination(0L, 3)
      val uriFilter = URLEncoder.encode("""{"@context": {"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}, "filter": {"op": "and", "value": [{"path": "rdf:type", "op": "eq", "value": "http://schema.org/QuantitativeValue"} ] } }""", "UTF-8")
      private val path = s"/data/${orgRef.id.id}/${domRef.id.id}/${schemaId.name}?from=${specificPagination.from}&size=${specificPagination.size}&deprecated=true&filter=$uriFilter"
      Get(path) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val results = responseAs[Results]
        results.total shouldEqual 5L
        results.results.size shouldEqual 3
        forAll(results.results.zipWithIndex) { case(result, idx) =>
          val schema = s"$baseUri/schemas/${schemaId.copy(version = Version(idx+5,0,0)).show}"
          val id = s"$schema/${baseUUID}0${idx+5}".replace("/schemas/", "/data/")
          result shouldEqual Result(id,
            Source(id,List(Link("self", id),Link("schema", schema))))
        }
        val nextUri = Uri(s"$base$path")
        results.links should contain allElementsOf
          List(Link("next", nextUri.withQuery(Query(nextUri.query().toMap + ("from" -> "3")))), Link("self", s"$base$path"))
      }
    }

    "return the instances that the selected instance is linked with as an outgoing link" in new Context  {
      indexInstances()
      private val path = s"/data/${instanceRef.id.show}/outgoing"
      Get(path) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val results = responseAs[Results]
        results.total shouldEqual 0L
        results.results.size shouldEqual 0
      }
    }

    "reject the request with 400 for outgoing links incorrect filter format" in new Context {
      private val filter = URLEncoder.encode("""{"filter": {}}""", "UTF-8")
      private val path = s"""/data/${instanceRef.id.show}/outgoing?filter=$filter"""
      Get(path) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val json: Json = responseAs[Json]
        json.hcursor.get[String]("field") shouldEqual Right("DownField(filter)/DownField(op)")
      }
    }

    "return the instances that the selected instance is linked with as an incoming link" in new Context  {
      indexInstances()
      private val path = s"/data/${instanceRef.id.show}/incoming"
      Get(path) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val results = responseAs[Results]
        results.total shouldEqual 0L
        results.results.size shouldEqual 0
      }
    }

    "reject the request with 400 for incoming links with incorrect filter format" in new Context {
      private val filter = URLEncoder.encode("""{"filter": {}}""", "UTF-8")
      private val path = s"""/data/${instanceRef.id.show}/incoming?filter=$filter"""
      Get(path) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val json: Json = responseAs[Json]
        json.hcursor.get[String]("field") shouldEqual Right("DownField(filter)/DownField(op)")
      }
    }

    "return 404 for a missing instance" in new Context {
      Get(s"/data/${instanceRef.id.show}a") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }


    "update an instance" in new Context {
      val valueJson = genJson()
      Put(s"/data/${instanceRef.id.show}?rev=${instanceRef.rev}", valueJson) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual instanceRefAsJson(InstanceRef(instanceRef.id, 2L))
      }
      instances.fetch(instanceRef.id).futureValue shouldEqual Some(Instance(instanceRef.id, 2L, valueJson, deprecated = false))
    }

    "reject updating an instance when it does not exists" in new Context {
      val wrongId = instanceRef.id.copy(id = "NotExist")
      Put(s"/data/${wrongId.show}?rev=${instanceRef.rev}", genJson()) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[InstanceDoesNotExist.type]
      }
    }

    "reject updating an instance when the new json data does not conform to the schema" in new Context{
      Put(s"/data/${instanceRef.id.show}?rev=${instanceRef.rev}", Json.obj()) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[ShapeConstraintViolations.type]
      }
    }

    "reject updating an instance with incorrect rev" in new Context {
      val newRev = instanceRef.rev + 10L
      Put(s"/data/${instanceRef.id.show}?rev=$newRev", genJson()) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
        responseAs[Error].code shouldEqual classNameOf[IncorrectRevisionProvided.type]
      }
    }

    "update an instance even when the schema is deprecated" in new Context {
      //deprecate schema
      schemas.deprecate(schemaId, 2L).futureValue

      //Update instance linked to a deprecated schema
      private val json = genJson()
      Put(s"/data/${instanceRef.id.show}?rev=${instanceRef.rev}", json) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual instanceRefAsJson(InstanceRef(instanceRef.id, 2L))
      }
      instances.fetch(instanceRef.id).futureValue shouldEqual Some(Instance(instanceRef.id, 2L, json, deprecated = false))
    }

    "deprecate an instance" in new Context {
      deprecateInstance(instanceRef)
      instances.fetch(instanceRef.id).futureValue shouldEqual Some(Instance(instanceRef.id, 2L, value, deprecated = true))
    }

    "reject the deprecation of an instance which is already deprecated" in new Context {
      deprecateInstance(instanceRef)

      Delete(s"/data/${instanceRef.id.show}?rev=2") ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[InstanceIsDeprecated.type]
      }
    }

    "reject updating an instance when it is deprecated" in new Context {
      deprecateInstance(instanceRef)

      Put(s"/data/${instanceRef.id.show}?rev=2", value) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[InstanceIsDeprecated.type]
      }
    }

    "create attachment to an instance and fetch instance" in new Context {
      val filename = "attachment.csv"
      val hash = "2fabc6464789da99d9fad59d27af24267f815fc1bb054e4520c78056aab285b9"
      val (multiPart, size) = multipartEntityAndFileSize(filename)
      val digest = Attachment.Digest(algorithm, hash)

      Put(s"/data/${instanceRef.id.show}/attachment?rev=${instanceRef.rev}", multiPart) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] shouldEqual
          instanceRefAsJson(InstanceRef(instanceRef.id, 2, Some(Info(filename, ContentTypes.`text/csv(UTF-8)`.toString(), Size(value = size), digest))))
      }

      Get(s"/data/${instanceRef.id.show}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "@id" -> Json.fromString(s"$baseUri/data/${instanceRef.id.show}"),
          "rev" -> Json.fromLong(2L),
          "deprecated" -> Json.fromBoolean(false)
        ).deepMerge(Info(filename, ContentTypes.`text/csv(UTF-8)`.toString(), Size(value = size), digest).asJson)
          .deepMerge(value)
        }

      deleteAttachments()
    }

    "create several attachments to an instance and fetch specific revision's attachment" in new Context {
      val filename = "attachment.csv"
      val hash = "2fabc6464789da99d9fad59d27af24267f815fc1bb054e4520c78056aab285b9"
      val (multiPart, size) = multipartEntityAndFileSize(filename)

      Put(s"/data/${instanceRef.id.show}/attachment?rev=${instanceRef.rev}", multiPart) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        val digest = Attachment.Digest(algorithm, hash)
        responseAs[Json] shouldEqual
          instanceRefAsJson(InstanceRef(instanceRef.id, 2, Some(Info(filename, ContentTypes.`text/csv(UTF-8)`.toString(), Size(value = size), digest))))
      }

      val filename2 = "attachment2.csv"
      val hash2 = "f904be328684a5270875c2c04d648b5eae317ee5ae9d3e7b09c9ec5d6a6416e2"
      val (multiPart2, size2) = multipartEntityAndFileSize(filename2)

      Put(s"/data/${instanceRef.id.show}/attachment?rev=2", multiPart2) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        val digest = Attachment.Digest(algorithm, hash2)
        responseAs[Json] shouldEqual
          instanceRefAsJson(InstanceRef(instanceRef.id, 3, Some(Info(filename2, ContentTypes.`text/csv(UTF-8)`.toString(), Size(value = size2), digest))))
      }

      //Fetch latest
      Get(s"/data/${instanceRef.id.show}/attachment?rev") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`text/csv(UTF-8)`
        responseEntity.dataBytes.toMat(digestSink)(Keep.right).run().futureValue.digest().map("%02x".format(_)).mkString shouldEqual hash2
      }

      //Fetch specific review
      Get(s"/data/${instanceRef.id.show}/attachment?rev=2") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`text/csv(UTF-8)`
        responseEntity.dataBytes.toMat(digestSink)(Keep.right).run().futureValue.digest().map("%02x".format(_)).mkString shouldEqual hash
      }

      //Fetch specific review
      Get(s"/data/${instanceRef.id.show}/attachment?rev=1") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
      deleteAttachments()
    }

    "prevent to delete a non existing attachment to an instance" in new Context {
      Delete(s"/data/${instanceRef.id.show}/attachment?rev=${instanceRef.rev}") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete attachment from an instance" in new Context {
      val filename = "attachment.csv"
      val hash = "2fabc6464789da99d9fad59d27af24267f815fc1bb054e4520c78056aab285b9"
      val (multiPart, size) = multipartEntityAndFileSize(filename)

      Put(s"/data/${instanceRef.id.show}/attachment?rev=${instanceRef.rev}", multiPart) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        val digest = Attachment.Digest(algorithm, hash)
        responseAs[Json] shouldEqual
          instanceRefAsJson(InstanceRef(instanceRef.id, 2, Some(Info(filename, ContentTypes.`text/csv(UTF-8)`.toString(), Size(value = size), digest))))
      }

      Delete(s"/data/${instanceRef.id.show}/attachment?rev=2") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual instanceRefAsJson(InstanceRef(instanceRef.id, 3L, None))
      }

      //Fetch specific review
      Get(s"/data/${instanceRef.id.show}/attachment?rev=2") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        contentType shouldEqual ContentTypes.`text/csv(UTF-8)`
        responseEntity.dataBytes.toMat(digestSink)(Keep.right).run().futureValue.digest().map("%02x".format(_)).mkString shouldEqual hash
      }

      //Fetch last review
      Get(s"/data/${instanceRef.id.show}/attachment") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }

      deleteAttachments()
    }
  }
}

object InstanceRoutesSpec {
  private val base = Uri("http://localhost")
  private val baseUri = base.copy(path = base.path / "v0")

  import cats.syntax.show._

  private def instanceRefAsJson(ref: InstanceRef) = Json.obj(
    "@id" -> Json.fromString(s"$baseUri/data/${ref.id.show}"),
    "rev" -> Json.fromLong(ref.rev)
  ).deepMerge(ref.attachment.map(at => at.asJson).getOrElse(Json.obj()))

  private def toCompact(value: String) =
    value.replaceAll(Pattern.quote(s"$baseUri/data/"), "")
}
