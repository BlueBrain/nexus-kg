package ch.epfl.bluebrain.nexus.kg.service.contexts

import java.time.Clock
import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.Uri
import cats.instances.try_._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.kg.service.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.service.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig.{HttpConfig, OperationsConfig}
import ch.epfl.bluebrain.nexus.kg.service.contexts.Resolver.IllegalImportsViolation
import ch.epfl.bluebrain.nexus.kg.service.contexts.Resolver.try_._
import ch.epfl.bluebrain.nexus.kg.service.operations.ResourceState
import ch.epfl.bluebrain.nexus.kg.service.projects.Project.Config
import ch.epfl.bluebrain.nexus.kg.service.projects.ProjectEvent.Value
import ch.epfl.bluebrain.nexus.kg.service.projects.{ProjectId, Projects}
import ch.epfl.bluebrain.nexus.kg.service.refs.RevisionedRef
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.{Matchers, TryValues, WordSpecLike}

import scala.util.Try

class ResolverSpec extends WordSpecLike with Matchers with Resources with TryValues {

  "A Resolver" when {
    val combined = jsonContentOf("/resolver/contexts/core/combined.json")
    "on local context" should {

      "failed to be resolved locally when link is not managed by the platform" in {
        val distribution =
          jsonContentOf("/resolver/contexts/core/distribution.json", Map(quote("{{base}}") -> "/resolver/other"))
        println(distribution.resolvedFor(Path("resolver")).failure.exception.asInstanceOf[CommandRejected].rejection)
        distribution.resolvedFor(Path("resolver")).failure.exception shouldEqual CommandRejected(
          IllegalImportsViolation(
            Set("Referenced context '/resolver/other/contexts/core/standards' is not managed in this platform")))
      }

      "be resolved locally" in {
        val distribution =
          jsonContentOf("/resolver/contexts/core/distribution.json", Map(quote("{{base}}") -> "/resolver"))
        distribution.resolvedFor(Path("resolver")).success.value shouldEqual combined
      }
    }

    "on stored context" should {
      implicit val caller     = AnonymousCaller(Anonymous())
      implicit val clock      = Clock.systemUTC
      implicit val opConfig   = OperationsConfig(32)
      val agg                 = MemoryAggregate("contexts")(ResourceState.Initial, Contexts.next, Contexts.eval).toF[Try]
      val aggProject          = MemoryAggregate("projects")(ResourceState.Initial, Projects.next, Projects.eval).toF[Try]
      val projects            = Projects(aggProject)
      implicit val contexts   = Contexts[Try](agg, projects)
      implicit val httpConfig = HttpConfig("127.0.0.1", 80, "v0", Uri("http://127.0.0.1/v0"))
      val distribution = jsonContentOf("/resolver/contexts/core/distribution.json",
                                       Map(quote("{{base}}") -> httpConfig.publicUri.toString()))

      "failed to be resolved when context linked does not exists" in {

        distribution.resolved.failure.exception shouldEqual CommandRejected(
          IllegalImportsViolation(
            Set("Referenced context 'http://127.0.0.1/v0/contexts/core/standards' does not exist")))
      }

      "be resolved" in {
        val projectId      = ProjectId("core").get
        val standards      = jsonContentOf("/resolver/contexts/core/standards.json")
        val standardsId    = ContextId(projectId, "standards").get
        val distributionId = ContextId(projectId, "distribution").get
        projects.create(projectId, Value(Json.obj(), Config(10))).success.value shouldEqual RevisionedRef(projectId, 1L)
        contexts.create(standardsId, standards).success.value shouldEqual RevisionedRef(standardsId, 1L)
        contexts.create(distributionId, distribution).success.value shouldEqual RevisionedRef(distributionId, 1L)

        distribution.resolved.success.value shouldEqual combined
      }

    }
  }
}
