String version = env.BRANCH_NAME
Boolean isRelease = version ==~ /v\d+\.\d+\.\d+.*/
Boolean isPR = env.CHANGE_ID != null

/*
 * Loops until the endpoint is up (true) or down (false), for 20 attempts maximum
 */
def wait(endpoint, up, attempt = 1) {
    if (attempt > 20) {
        error("Invalid response from $endpoint after 20 attempts")
    }
    def response = httpRequest url: endpoint, validResponseCodes: '100:599' // prevents an exception to be thrown on 500 codes
    if (up && response.status == 200) {
        echo "$endpoint is up"
        return
    } else if (!up && response.status == 503) {
        echo "$endpoint is down"
        return
    } else {
        sleep 10
        wait(endpoint, up, attempt + 1)
    }
}

def buildResults = [:]

pipeline {
    agent { label 'slave-sbt' }
    options {
        timeout(time: 30, unit: 'MINUTES')
    }
    environment {
        ENDPOINT = sh(script: 'oc env statefulset/kg -n bbp-nexus-dev --list | grep SERVICE_DESCRIPTION_URI', returnStdout: true).split('=')[1].trim()
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
                sleep 10
                wait(ENDPOINT, false)
                sh "oc scale statefulset kg --replicas=1 --namespace=bbp-nexus-dev"
                sleep 120 // service readiness delay is set to 2 minutes
                openshiftVerifyService namespace: 'bbp-nexus-dev', svcName: 'kg', verbose: 'false'
                wait(ENDPOINT, true)
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
            echo "A job failed"
            mail bcc: '', body: "<b>Build results for ${currentBuild.displayName.toString()}</b><br><br>Current build: ${currentBuild.displayName.toString()} ${currentBuild.results.toString()} <br>Integration-Tests build ${results.toString()}", cc: '', charset: 'UTF-8', from: '', mimeType: 'text/html', replyTo: '', subject: "ERROR CI: Project name -> ${currentBuild.displayName.toString()}", to: "${env.NOTIFICATION_EMAIL}";
        }
    }
}