String version = env.BRANCH_NAME
Boolean isRelease = version ==~ /v\d+\.\d+\.\d+.*/
Boolean isPR = env.CHANGE_ID != null

pipeline {
    agent { label 'slave-sbt' }
    options {
        timeout(time: 30, unit: 'MINUTES')
    }
    stages {
        stage("Review") {
            when {
                expression { isPR }
            }
            parallel {
                stage("StaticAnalysis") {
                    steps {
                        node("slave-sbt") {
                            checkout scm
                            sh 'sbt clean scalafmtCheck scalafmtSbtCheck test:scalafmtCheck compile test:compile scapegoat'
                        }
                    }
                }
                stage("Tests & Coverage") {
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
        stage("Build & Publish Artifacts") {
            when {
                expression { !isPR }
            }
            steps {
                checkout scm
                sh 'sbt releaseEarly universal:packageZipTarball'
                stash name: "service", includes: "target/universal/kg*.tgz"
            }
        }
        stage("Build Image") {
            when {
                expression { !isPR }
            }
            steps {
                unstash name: "service"
                sh "mv target/universal/kg*.tgz ./kg.tgz"
                sh "oc start-build kg-build --from-file=kg.tgz --wait"
            }
        }
        stage("Redeploy & Test") {
            when {
                expression { !isPR && !isRelease }
            }
            steps {
                sh "oc scale statefulset kg --replicas=0 --namespace=bbp-nexus-dev"
                sh "oc wait pods/kg-0 --for=delete --namespace=bbp-nexus-dev --timeout=3m"
                sh "oc scale statefulset kg --replicas=1 --namespace=bbp-nexus-dev"
                sh "oc wait pods/kg-0 --for condition=ready --namespace=bbp-nexus-dev --timeout=4m"
                build job: 'nexus/nexus-tests/master', parameters: [booleanParam(name: 'run', value: true)], wait: true
            }
        }
        stage("Tag Images") {
            when {
                expression { isRelease }
            }
            steps {
                openshiftTag srcStream: 'kg', srcTag: 'latest', destStream: 'kg', destTag: version.substring(1), verbose: 'false'
            }
        }
       stage("Push to Docker Hub") {
           when {
               expression { isRelease }
           }
           steps {
               unstash name: "service"
               sh "mv target/universal/kg*.tgz ./kg.tgz"
               sh "oc start-build nexus-kg-build --from-file=kg.tgz --wait"
            }
        }
        stage("Report Coverage") {
            when {
                expression { !isPR }
            }
            steps {
                checkout scm
                sh "sbt clean coverage test coverageReport coverageAggregate"
                sh "curl -s https://codecov.io/bash >> ./coverage.sh"
                sh "bash ./coverage.sh -t `oc get secrets codecov-secret --template='{{.data.nexus_kg}}' | base64 -d`"
            }
            post {
                always {
                    junit 'target/test-reports/TEST*.xml'
                }
            }
        }
    }
}