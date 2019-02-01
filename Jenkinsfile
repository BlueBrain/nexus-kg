String version = env.BRANCH_NAME
Boolean isRelease = version ==~ /v\d+\.\d+\.\d+.*/
Boolean isPR = env.CHANGE_ID != null

def buildResults = [:]

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
        stage("Build Snapshot & Deploy") {
            when {
                expression { !isPR && !isRelease }
            }
            steps {
                checkout scm
                sh 'sbt releaseEarly universal:packageZipTarball'
                sh "mv target/universal/kg*.tgz ./kg.tgz"
                sh "oc start-build kg-build --from-file=kg.tgz --follow"
                sh "oc scale statefulset kg --replicas=0 --namespace=bbp-nexus-dev"
                sh "oc wait pods/kg-0 --for=delete --namespace=bbp-nexus-dev --timeout=3m"
                sh "oc scale statefulset kg --replicas=1 --namespace=bbp-nexus-dev"
                sh "oc wait pods/kg-0 --for condition=ready --namespace=bbp-nexus-dev --timeout=4m"
            }
        }
        stage('Run integration tests') {
            when {
                expression { !isPR && !isRelease }
            }
            steps {
                script {
                    def jobBuild = build job:  'nexus/nexus-tests/master', parameters: [booleanParam(name: 'run', value: true)], wait: true
                    def jobResult = jobBuild.getResult()
                    echo "Integration tests of 'nexus-tests' returned result: ${jobResult}"

                    s['nexus-tests']buildResult = jobResult

                    if (jobResult != 'SUCCESS') {
                        error("nexus-tests failed with result: ${jobResult}")
                    }
                }
            }
        }
        stage("Build & Publish Release") {
            when {
                expression { isRelease }
            }
            steps {
                checkout scm
                sh 'sbt releaseEarly universal:packageZipTarball'
                sh "mv target/universal/kg*.tgz ./kg.tgz"
                echo "Pushing to internal image registry..."
                sh "oc start-build kg-build --from-file=kg.tgz --wait"
                openshiftTag srcStream: 'kg', srcTag: 'latest', destStream: 'kg', destTag: version.substring(1), verbose: 'false'
                echo "Pushing to Docker Hub..."
                sh "oc start-build nexus-kg-build --from-file=kg.tgz --wait"
            }
        }
    }
    post {
        always {
            echo "Build results: ${buildResults.toString()}"
        }
        success {
            echo "All builds completed OK"
        }
        failure {
            echo "Job failed"
        }
    }
}