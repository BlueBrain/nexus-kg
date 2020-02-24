package ch.epfl.bluebrain.nexus.kg.serializers

import java.nio.file.Path

import com.esotericsoftware.kryo.{Kryo, Serializer}
import com.esotericsoftware.kryo.io.{Input, Output}
import io.altoo.akka.serialization.kryo.DefaultKryoInitializer
import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo

class PathSerializer extends Serializer[Path] {

  override def write(kryo: Kryo, output: Output, path: Path): Unit =
    output.writeString(path.toString)

  override def read(kryo: Kryo, input: Input, `type`: Class[Path]): Path =
    Path.of(input.readString())
}


class KryoSerializerInit extends DefaultKryoInitializer {

  override def postInit(kryo: ScalaKryo): Unit = {
    super.postInit(kryo)
    kryo.addDefaultSerializer(classOf[Path], classOf[PathSerializer])
    kryo.register(classOf[Path], new PathSerializer)
    ()
  }
}
