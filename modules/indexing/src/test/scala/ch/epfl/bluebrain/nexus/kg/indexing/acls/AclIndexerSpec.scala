package ch.epfl.bluebrain.nexus.kg.indexing.acls

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.instances.future._
import cats.instances.string._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Event._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.iam.acls.{AccessControlList, Meta, Path, Permissions}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.{GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.commons.iam.identity.{Identity, IdentityId}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlClient
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.indexing.IndexingVocab.PrefixMapping._
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.query.SearchVocab.SelectTerms._
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, IndexerFixture, Qualifier}
import org.apache.jena.query.ResultSet
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

@DoNotDiscover
class AclIndexerSpec(blazegraphPort: Int)
    extends TestKit(ActorSystem("AclIndexerSpec"))
    with IndexerFixture
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Randomness
    with Resources
    with Inspectors
    with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(3 seconds, 100 milliseconds)

  private implicit val ec: ExecutionContextExecutor = system.dispatcher
  private implicit val mt: ActorMaterializer        = ActorMaterializer()

  private implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient
  private implicit val rs: HttpClient[Future, ResultSet] =
    HttpClient.withAkkaUnmarshaller[ResultSet]

  private val base         = s"http://$localhost/v0"
  private val baseUri: Uri = s"http://$localhost:$blazegraphPort/blazegraph"

  private val settings @ AclIndexingSettings(index, aclsBase, aclsBaseNs, nexusVocBase) =
    AclIndexingSettings(genString(length = 6), base, s"$base/acls/graphs", s"$base/voc/nexus/core")

  private implicit val stringQualifier: ConfiguredQualifier[String] = Qualifier.configured[String](nexusVocBase)

  private def triples(client: SparqlClient[Future]): Future[List[(String, String, String)]] =
    client.query(index, "SELECT * { ?s ?p ?o }").map { rs =>
      rs.asScala.toList.map { qs =>
        val obj = {
          val node = qs.get("?o")
          if (node.isLiteral) node.asLiteral().getLexicalForm
          else node.asResource().toString
        }
        (qs.get(s"?$subject").toString, qs.get("?p").toString, obj)
      }
    }

  private def expectedTriples(id: String, identities: Set[Identity]): List[(String, String, String)] =
    identities.map { identity =>
      (id, "read" qualifyAsStringWith nexusVocBase, identity.id.id)
    }.toList ++ List((id, rdfTypeKey, "Acl".qualifyAsString))

  "A AclIndexer" should {
    val client  = SparqlClient[Future](baseUri)
    val indexer = AclIndexer(client, settings)

    val user   = UserRef(IdentityId(s"$base/realms/realm/users/1234"))
    val group  = GroupRef(IdentityId(s"$base/realms/realm/groups/admin"))
    val group2 = GroupRef(IdentityId(s"$base/realms/realm/groups/core"))
    val group3 = GroupRef(IdentityId(s"$base/realms/realm/groups/other"))
    val group4 = GroupRef(IdentityId(s"$base/realms/realm/groups/another"))

    val meta = Meta(user, Instant.ofEpochMilli(1))

    val orgId    = OrgId("org")
    val domainId = DomainId(orgId, "dom")
    val schema   = SchemaId(domainId, "name", Version(1, 0, 0))
    val instance = InstanceId(schema, UUID.randomUUID().toString)

    "index a PermissionsCreated event" in {
      client.createIndex(index, properties).futureValue
      val path = Path("kg") ++ Path(orgId.show)
      indexer(
        PermissionsCreated(
          path,
          AccessControlList(user -> Permissions(Write), group -> Permissions(Read), group2 -> Permissions(Read)),
          meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 3
      rs shouldEqual expectedTriples(s"$base/organizations/${orgId.show}", Set(group, group2))
    }

    "index a PermissionsAdded event on organizations" in {
      val path = Path("kg") ++ Path(orgId.show)
      indexer(PermissionsAdded(path, group3, Permissions(Read), meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 4
      rs shouldEqual expectedTriples(s"$base/organizations/${orgId.show}", Set(group, group2, group3))
    }

    "Do not index a PermissionsAdded event on organizations when the path does not start with kg" in {
      val path = Path(orgId.show)
      indexer(PermissionsAdded(path, group4, Permissions(Read), meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 4
      rs shouldEqual expectedTriples(s"$base/organizations/${orgId.show}", Set(group, group2, group3))
    }

    "Do not index a PermissionsAdded event on organizations when the path is only /kg" in {
      val path = Path("kg")
      indexer(PermissionsAdded(path, group4, Permissions(Read), meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 4
      rs shouldEqual expectedTriples(s"$base/organizations/${orgId.show}", Set(group, group2, group3))
    }

    "index a PermissionsSubtracted event on organizations" in {
      val path = Path("kg") ++ Path(orgId.show)
      indexer(PermissionsSubtracted(path, group, Permissions(Read), meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 3
      rs shouldEqual expectedTriples(s"$base/organizations/${orgId.show}", Set(group2, group3))
    }

    "index a PermissionsAdded event on instances" in {
      val path = Path("kg") ++ Path(instance.show)
      indexer(PermissionsAdded(path, user, Permissions(Read), meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 5
      val expectedAllTriples = expectedTriples(s"$base/organizations/${orgId.show}", Set(group2, group3))
      rs shouldEqual expectedAllTriples ++ expectedTriples(s"$base/data/${instance.show}", Set(user))
    }

    "index a PermissionsCleared event on organizations" in {
      val path = Path("kg") ++ Path(orgId.show)
      indexer(PermissionsCleared(path, meta)).futureValue
      val rs = triples(client).futureValue
      rs.size shouldEqual 2
      rs shouldEqual expectedTriples(s"$base/data/${instance.show}", Set(user))
    }
  }
}
