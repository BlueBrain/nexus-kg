package ch.epfl.bluebrain.nexus.kg

import ch.epfl.bluebrain.nexus.kg.core.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import io.circe.Decoder
import shapeless.Typeable

object ElasticIdDecoder {

  /**
    * Create and implicit ID decoder which will try to decode `A` from `@id` field in JSON object using
    * `ConfiguredQualifier[A]`
    * @param Q    ConfiguredQualifier for A
    * @param T    Typeable for A
    * @tparam A   type of object to be decooded
    * @return     `Deocder` for `S`
    */
  final implicit def elasticIdDecoder[A](implicit Q: ConfiguredQualifier[A], T: Typeable[A]): Decoder[A] =
    Decoder.decodeJson.emap { json =>
      json.hcursor
        .get[String]("@id")
        .toOption
        .flatMap(_.unqualify[A])
        .map(Right(_))
        .getOrElse(Left(s"Couldn't decode ${T.describe} from '@id' in ${json.noSpaces}"))
    }

}
