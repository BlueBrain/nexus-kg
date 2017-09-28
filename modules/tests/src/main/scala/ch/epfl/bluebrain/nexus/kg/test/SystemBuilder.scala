package ch.epfl.bluebrain.nexus.kg.test

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

/**
  * Various functions for building actor systems to be used during testing.
  */
object SystemBuilder {

  /**
    * Constructs an actor system with akka persistence configured to cassandra, clustering enabled and
    * Blazegraph port set to one of the available ports.
    *
    * @param name            the name of the actor system
    * @param cassandraPort   the local port where Cassandra embedded instance is running
    * @param blazegraphPort  the local port where Blazegraph embedded instance is running
    * @return an actor system with akka persistence configured to cassandra and clustering enabled
    */
  final def initConfig(name: String, cassandraPort: Int, blazegraphPort: Int): ActorSystem = {
    val config = ConfigFactory.parseString(
      s"""
         |test.cassandra-port = $cassandraPort
         |test.blazegraph-port = $blazegraphPort
       """.stripMargin)
      .withFallback(ConfigFactory.parseResources("cluster-test.conf"))
      .withFallback(ConfigFactory.parseResources("cassandra-test.conf"))
      .withFallback(ConfigFactory.parseResources("sparql-test.conf"))
      .withFallback(ConfigFactory.parseResources("app-test.conf"))
      .withFallback(ConfigFactory.load())
      .resolve()
    ActorSystem(name, config)
  }
}


