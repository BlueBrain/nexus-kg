package ch.epfl.bluebrain.nexus.kg.indexing

import org.scalatest.{BeforeAndAfterAll, Suites}
import ch.epfl.bluebrain.nexus.kg.core.Randomness._
import ch.epfl.bluebrain.nexus.kg.indexing.domains.DomainIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.instances.InstanceIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.organizations.OrganizationIndexerSpec
import ch.epfl.bluebrain.nexus.kg.indexing.query.SparqlQuerySpec
import ch.epfl.bluebrain.nexus.kg.indexing.schemas.SchemaIndexerSpec
import com.bigdata.rdf.sail.webapp.NanoSparqlServer

/**
  * Bundles all suites that depend on a running blazegraph instance.
  */
class BlazegraphSpec extends Suites with BeforeAndAfterAll {

  private val port = freePort()

  override val nestedSuites = Vector(
    new InstanceIndexerSpec(port),
    new SchemaIndexerSpec(port),
    new DomainIndexerSpec(port),
    new OrganizationIndexerSpec(port),
    new SparqlQuerySpec(port))

  private val server = {
    System.setProperty("jetty.home", getClass.getResource("/war").toExternalForm)
    NanoSparqlServer.newInstance(port, null, null)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    server.start()
  }

  override protected def afterAll(): Unit = {
    server.stop()
    super.afterAll()
  }
}
