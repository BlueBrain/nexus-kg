package ch.epfl.bluebrain.nexus.kg.indexing

import org.scalatest.{BeforeAndAfterAll, Suites}
import ch.epfl.bluebrain.nexus.commons.test.Randomness.freePort
import ch.epfl.bluebrain.nexus.kg.indexing.acls.AclIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.contexts.ContextSparqlIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.domains.DomainSparqlIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.instances.InstanceIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.organizations.OrganizationSparqlIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.query.SparqlQuerySpec
import ch.epfl.bluebrain.nexus.kg.indexing.schemas.SchemaIndexerSpec
import com.bigdata.rdf.sail.webapp.NanoSparqlServer

import scala.concurrent.duration._

/**
  * Bundles all suites that depend on a running blazegraph instance.
  */
trait BlazegraphSpecLike extends Suites with BeforeAndAfterAll {

  val port = freePort()

  private val server = {
    System.setProperty("jetty.home", getClass.getResource("/war").toExternalForm)
    NanoSparqlServer.newInstance(port, null, null)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    server.setStopTimeout(1.second.toMillis)
    server.setStopAtShutdown(true)
    server.start()
  }

  override protected def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}

class BlazeGraphIndexingSpec extends BlazegraphSpecLike {
  override val nestedSuites = Vector(
    new AclIndexerSpec(port),
    new ContextSparqlIndexerSpec(port),
    new InstanceIndexerSpec(port),
    new SchemaIndexerSpec(port),
    new DomainSparqlIndexerSpec(port),
    new OrganizationSparqlIndexerSpec(port),
    new SparqlQuerySpec(port)
  )
}
