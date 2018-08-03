package ch.epfl.bluebrain.nexus.kg.resources

import ch.epfl.bluebrain.nexus.kg.resources.Ref._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Urn}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.{EitherValues, Inspectors, Matchers, WordSpecLike}

class RefSpec extends WordSpecLike with Matchers with Inspectors with EitherValues {

  "A Ref" should {

    "be constructed from an AbsoluteIri" in {
      val list = List[(AbsoluteIri, Ref)](
        url"http://ex.com?rev=1&other=value".value          -> Revision(url"http://ex.com?other=value", 1L),
        url"http://ex.com?rev=1".value                      -> Revision(url"http://ex.com", 1L),
        url"http://ex.com?tag=this&other=value".value       -> Tag(url"http://ex.com?other=value", "this"),
        url"http://ex.com?rev=1&tag=this&other=value".value -> Revision(url"http://ex.com?other=value", 1L),
        url"http://ex.com?other=value".value                -> Latest(url"http://ex.com?other=value"),
        url"http://ex.com#fragment".value                   -> Latest(url"http://ex.com#fragment"),
        Urn("urn:ex:a/b/c").right.value                     -> Latest(Urn("urn:ex:a/b/c").right.value),
        Urn("urn:ex:a/b/c?=rev=1").right.value              -> Revision(Urn("urn:ex:a/b/c").right.value, 1L),
        Urn("urn:ex:a?=tag=this&other=value").right.value   -> Tag(Urn("urn:ex:a?=other=value").right.value, "this")
      )
      forAll(list) {
        case (iri, ref) => Ref(iri) shouldEqual ref
      }
    }

    "print properly" in {
      (Latest(url"http://ex.com#fragment"): Ref).show shouldEqual url"http://ex.com#fragment".value.show
      (Revision(url"http://ex.com?other=value", 1L): Ref).show shouldEqual url"http://ex.com?other=value".value.show + s" @ rev: '1'"
      (Tag(url"http://ex.com?other=value", "this"): Ref).show shouldEqual url"http://ex.com?other=value".value.show + s" @ tag: 'this'"
    }
  }

}
