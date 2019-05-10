package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}

import cats.data.EitherT
import cats.effect.{IO, Timer}
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts.resolverCtx
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{Schemas, Settings}
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest._

import scala.concurrent.duration._

class ResolverIndexerMappingSpec
    extends ActorSystemFixture("ResolverIndexerMappingSpec")
    with WordSpecLike
    with Matchers
    with IOEitherValues
    with IOOptionValues
    with BeforeAndAfter
    with test.Resources
    with IdiomaticMockito
    with TestHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 15 milliseconds)

  private implicit val appConfig          = Settings(system).appConfig
  private implicit val indexingConfig     = appConfig.keyValueStore.indexing
  private implicit val ioTimer: Timer[IO] = IO.timer(system.dispatcher)

  private val resolvers             = mock[Resolvers[IO]]
  private implicit val projectCache = mock[ProjectCache[IO]]
  private val mapper                = new ResolverIndexerMapping(resolvers)

  before {
    Mockito.reset(resolvers)
    Mockito.reset(projectCache)
  }

  "An Resolver event mapping function" when {
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val base                  = Iri.absolute("http://example.com").right.value
    val voc                   = base + "voc"
    val iri                   = base + "id"
    val subject               = base + "anonymous"
    val projectRef            = ProjectRef(genUUID)
    val id                    = Id(projectRef, iri)
    implicit val project = Project(id.value,
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
                                   Instant.now(clock),
                                   subject,
                                   Instant.now(clock),
                                   subject)
    val schema = Ref(Schemas.resolverSchemaUri)

    val types = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)

    val json      = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val resourceV = simpleV(id, json, rev = 2, schema = schema, types = types)
    val resolver  = Resolver(resourceV).right.value
    val ev =
      Created(id, OrganizationRef(project.organizationUuid), schema, types, json, clock.instant(), Anonymous)

    "return a resolver" in {
      projectCache.get(projectRef) shouldReturn IO.pure(Some(project))
      resolvers.fetchResolver(id) shouldReturn EitherT.rightT[IO, Rejection](resolver)

      mapper(ev).some shouldEqual resolver
    }

    "return none when the resource cannot be found" in {
      projectCache.get(projectRef) shouldReturn IO.pure(Some(project))
      resolvers.fetchResolver(id) shouldReturn EitherT.leftT[IO, Resolver](NotFound(id.ref): Rejection)
      mapper(ev).ioValue shouldEqual None
    }
  }
}
