pipeline {
  options {
    buildDiscarder logRotator(artifactDaysToKeepStr: '1', artifactNumToKeepStr: '60', daysToKeepStr: '60', numToKeepStr: '60')
    ansiColor('xterm')
    timestamps()
    withCredentials([[$class       : 'AmazonWebServicesCredentialsBinding',
                      credentialsId: 'in-a-p-jen-codeartifact']])
  }
  
  environment {
    REGION = 'us-east-1'
    DOMAIN = "spanning"
    REPOSITORY_NAME = "shared"
    REPOSITORY_FORMAT = "maven"
    BUILD_IMAGE = 'openjdk:17.0.2'
    TIMESTAMP = (new Date()).format('yyyyMMdd-HHmm')
  }
  
  agent {
    label 'build'
  }
  
  stages {
    stage('Build') {
      environment {
        CODE_ARTIFACT_AUTH_TOKEN = sh(returnStdout: true,
          label: 'Get CodeArtifact auth token',
          script: '''#!/bin/bash
                     aws codeartifact get-authorization-token \
                        --region $REGION \
                        --domain $DOMAIN \
                        --query authorizationToken \
                        --output text \
                        --duration-seconds 900''').trim()
        CODE_ARTIFACT_URL = sh(returnStdout: true,
          label: 'Get CodeArtifact URL',
          script: '''#!/bin/bash
                     aws codeartifact get-repository-endpoint \
                       --region $REGION \
                       --domain $DOMAIN \
                       --repository $REPOSITORY_NAME \
                       --format $REPOSITORY_FORMAT \
                       --output text''').trim()
        BUILD_VERSION = "${TIMESTAMP}.${GIT_COMMIT.substring(0, 9)}.${BUILD_NUMBER}"
      }
      steps {
        writeFile(file: 'settings.xml',
          text: """
                          <settings>
                            <servers>
                              <server>
                                <id>codeartifact</id>
                                <username>aws</username>
                                <password>${CODE_ARTIFACT_AUTH_TOKEN}</password>
                              </server>
                            </servers>
                          </settings>
                        """)
        withDockerContainer(image: BUILD_IMAGE) {
          withMaven(mavenOpts: '-XX:+UseParallelGC') {
            withEnv(["PATH=${MVN_CMD_DIR}:${PATH}"]) {
              sh(label: 'maven set version',
                script: "./mvnw versions:set -DnewVersion=${BUILD_VERSION} -DprocessAllModules -DgenerateBackupPoms=false")
              sh(label: 'maven package',
                script: "./mvnw clean package -DskipTests")
              sh(label: "maven publish",
                script: "./mvnw clean deploy -s settings.xml -DskipTests")
            }
          }
        }
      }
    }
  }
}
