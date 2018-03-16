resolvers += Resolver.bintrayRepo("bbp", "nexus-releases")

addSbtPlugin("ch.epfl.bluebrain.nexus" % "sbt-nexus"           % "0.10.5")
addSbtPlugin("com.eed3si9n"            % "sbt-buildinfo"       % "0.7.0")
addSbtPlugin("com.lightbend.paradox"   % "sbt-paradox"         % "0.3.2")
addSbtPlugin("ch.epfl.bluebrain.nexus" % "sbt-nexus-workbench" % "0.3.0")
