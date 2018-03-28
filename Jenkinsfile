def version = env.BRANCH_NAME

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
                            sh 'sbt clean scalafmtCheck scalafmtSbtCheck test:scalafmtCheck scapegoat'
                        }
                    }
                }
                stage("Tests/Coverage") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh "sbt clean coverage test coverageReport coverageAggregate"
                            sh "curl -s https://codecov.io/bash >> ./coverage.sh"
                            sh "bash ./coverage.sh -t `oc get secrets codecov-secret --template='{{.data.nexus_kg}}' | base64 -d`"
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
                    sh 'sbt releaseEarly universal:packageZipTarball'
                    stash name: "service", includes: "modules/service/target/universal/kg-service-*.tgz"
                }
            }
        }
        stage("Build Image") {
            when {
                expression { version ==~ /v\d+\.\d+\.\d+.*/ }
            }
            steps {
                node("slave-sbt") {
                    unstash name: "service"
                    sh "mv modules/service/target/universal/kg-service-*.tgz ./kg-service.tgz"
                    sh "oc start-build kg-build --from-file=kg-service.tgz --follow"
                    openshiftTag srcStream: 'kg', srcTag: 'latest', destStream: 'kg', destTag: version.substring(1), verbose: 'false'
                }
            }
        }
        stage("Report Coverage") {
            when {
                expression { env.CHANGE_ID == null }
            }
            steps {
                node("slave-sbt") {
                    checkout scm
                    sh "sbt clean coverage test coverageReport coverageAggregate"
                    sh "curl -s https://codecov.io/bash >> ./coverage.sh"
                    sh "bash ./coverage.sh -t `oc get secrets codecov-secret --template='{{.data.nexus_kg}}' | base64 -d`"
                }
            }
        }
    }
}