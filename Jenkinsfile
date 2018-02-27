pipeline {
    agent none

    stages {
        stage("Review") {
            when {
                expression { env.CHANGE_ID != null }
            }
            parallel {
                stage("StaticAnalysis") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh 'sbt clean scalafmtSbtCheck scapegoat'
                        }
                    }
                }
                stage("Tests/Coverage") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh 'sbt clean coverage test coverageReport coverageAggregate'
                        }
                    }
                }
            }
        }
        stage("Release") {
            when {
                expression { env.CHANGE_ID == null }
            }
            steps {
                node("slave-sbt") {
                    checkout scm
                    sh 'sbt releaseEarly'
                }
            }
        }
    }
}
