package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}

import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlResults.Binding
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.indexing.SparqlLink.{SparqlExternalLink, SparqlResourceLink}
import org.scalatest.{Matchers, OptionValues, WordSpecLike}
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri

class SparqlLinkSpec extends WordSpecLike with Matchers with OptionValues {

  "A SparqlLink" should {

    val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

    val id       = url"http://example.com/id".value
    val property = url"http://example.com/friend".value

    "build SparqlExternalLink from SPARQL response" in {
      val bindings = Map("s" -> Binding("uri", id.asString), "property" -> Binding("uri", property.asString))
      SparqlExternalLink(bindings).value shouldEqual SparqlExternalLink(id, property)
    }

    "build SparqlResourceLink from SPARQL response" in {
      val self    = url"http://127.0.0.1:8080/v1/resources/myorg/myproject/_/id".value
      val project = url"http://127.0.0.1:8080/v1/projects/myorg/myproject/".value
      val author  = url"http://127.0.0.1:8080/v1/realms/myrealm/users/me".value
      val bindings = Map(
        "_rev"           -> Binding("literal", "1", datatype = Some(xsd.long.value.asString)),
        "_self"          -> Binding("uri", self.asString),
        "_project"       -> Binding("uri", project.asString),
        "type"           -> Binding("uri", nxv.Resolver.asString),
        "_constrainedBy" -> Binding("uri", unconstrainedSchemaUri.asString),
        "_createdBy"     -> Binding("uri", author.asString),
        "_updatedBy"     -> Binding("uri", author.asString),
        "_createdAy"     -> Binding("uri", author.asString),
        "_createdAt"     -> Binding("literal", clock.instant().toString, datatype = Some(xsd.dateTime.value.asString)),
        "_updatedAt"     -> Binding("literal", clock.instant().toString, datatype = Some(xsd.dateTime.value.asString)),
        "_deprecated"    -> Binding("literal", "false", datatype = Some(xsd.boolean.value.asString))
      )
      SparqlResourceLink(bindings, SparqlExternalLink(id, property), Set(nxv.Schema)).value shouldEqual
        SparqlResourceLink(id,
                           project,
                           self,
                           1L,
                           Set[AbsoluteIri](nxv.Schema, nxv.Resolver),
                           false,
                           clock.instant(),
                           clock.instant(),
                           author,
                           author,
                           unconstrainedRef,
                           property)
    }
  }

}
