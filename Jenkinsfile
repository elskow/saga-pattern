pipeline {
    agent any

    triggers {
        githubPush()
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timeout(time: 120, unit: 'MINUTES')
        timestamps()
        disableConcurrentBuilds()
    }

    parameters {
        choice(
            name: 'BUILD_SCOPE',
            choices: ['all', 'choreography-only', 'orchestration-only', 'changed-only'],
            description: 'Which services to build'
        )
        booleanParam(
            name: 'SKIP_TESTS',
            defaultValue: false,
            description: 'Skip running tests'
        )
        booleanParam(
            name: 'FORCE_REBUILD',
            defaultValue: false,
            description: 'Force rebuild even if no changes detected'
        )
    }

    environment {
        REGISTRY = 'ghcr.io'
        IMAGE_PREFIX = "ghcr.io/elskow/saga-pattern"
        GHCR_CREDS = credentials('github-ghcr-creds')
        DISCORD_WEBHOOK = credentials('discord-webhook-url')
        MAVEN_OPTS = '-Xmx4g -XX:+UseG1GC'
        FAILED_SERVICES = ''
        MENTION = '<@594544493869531292>'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                script {
                    env.SHORT_SHA = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                    env.BRANCH_NAME = sh(script: 'git rev-parse --abbrev-ref HEAD', returnStdout: true).trim()

                    // Detect changed services for 'changed-only' build scope
                    if (params.BUILD_SCOPE == 'changed-only' && !params.FORCE_REBUILD) {
                        def changedFiles = sh(
                            script: "git diff --name-only HEAD~1 HEAD || echo ''",
                            returnStdout: true
                        ).trim()

                        env.BUILD_CHOREOGRAPHY_ORDER = changedFiles.contains('choreography-saga/order-service') || changedFiles.contains('common/') ? 'true' : 'false'
                        env.BUILD_CHOREOGRAPHY_PAYMENT = changedFiles.contains('choreography-saga/payment-service') || changedFiles.contains('common/') ? 'true' : 'false'
                        env.BUILD_CHOREOGRAPHY_INVENTORY = changedFiles.contains('choreography-saga/inventory-service') || changedFiles.contains('common/') ? 'true' : 'false'
                        env.BUILD_CHOREOGRAPHY_SHIPPING = changedFiles.contains('choreography-saga/shipping-service') || changedFiles.contains('common/') ? 'true' : 'false'
                        env.BUILD_ORCHESTRATION_ORDER = changedFiles.contains('orchestration-saga/order-service') || changedFiles.contains('common/') ? 'true' : 'false'
                        env.BUILD_ORCHESTRATION_PAYMENT = changedFiles.contains('orchestration-saga/payment-service') || changedFiles.contains('common/') ? 'true' : 'false'
                        env.BUILD_ORCHESTRATION_INVENTORY = changedFiles.contains('orchestration-saga/inventory-service') || changedFiles.contains('common/') ? 'true' : 'false'
                        env.BUILD_ORCHESTRATION_SHIPPING = changedFiles.contains('orchestration-saga/shipping-service') || changedFiles.contains('common/') ? 'true' : 'false'
                    } else {
                        // Build all or based on scope
                        def buildChoreography = params.BUILD_SCOPE == 'all' || params.BUILD_SCOPE == 'choreography-only' || params.FORCE_REBUILD
                        def buildOrchestration = params.BUILD_SCOPE == 'all' || params.BUILD_SCOPE == 'orchestration-only' || params.FORCE_REBUILD

                        env.BUILD_CHOREOGRAPHY_ORDER = buildChoreography ? 'true' : 'false'
                        env.BUILD_CHOREOGRAPHY_PAYMENT = buildChoreography ? 'true' : 'false'
                        env.BUILD_CHOREOGRAPHY_INVENTORY = buildChoreography ? 'true' : 'false'
                        env.BUILD_CHOREOGRAPHY_SHIPPING = buildChoreography ? 'true' : 'false'
                        env.BUILD_ORCHESTRATION_ORDER = buildOrchestration ? 'true' : 'false'
                        env.BUILD_ORCHESTRATION_PAYMENT = buildOrchestration ? 'true' : 'false'
                        env.BUILD_ORCHESTRATION_INVENTORY = buildOrchestration ? 'true' : 'false'
                        env.BUILD_ORCHESTRATION_SHIPPING = buildOrchestration ? 'true' : 'false'
                    }
                }
            }
        }

        stage('Test') {
            when {
                expression { return !params.SKIP_TESTS }
            }
            steps {
                sh 'mvn clean verify --batch-mode -U --fail-at-end'
            }
            post {
                always {
                    junit testResults: '**/target/surefire-reports/*.xml', allowEmptyResults: true
                    junit testResults: '**/target/failsafe-reports/*.xml', allowEmptyResults: true
                }
            }
        }

        stage('Install Common Modules') {
            steps {
                sh 'mvn clean install -N -DskipTests --batch-mode -U'
                sh 'mvn clean install -pl common -DskipTests --batch-mode -U'
            }
        }

        stage('Docker Login') {
            steps {
                sh "echo ${GHCR_CREDS_PSW} | docker login ghcr.io -u ${GHCR_CREDS_USR} --password-stdin"
            }
        }

        stage('Build & Publish Services') {
            parallel {
                stage('choreography-order-service') {
                    when { expression { return env.BUILD_CHOREOGRAPHY_ORDER == 'true' } }
                    steps {
                        buildAndPushService('choreography-saga/order-service', 'choreography-order-service')
                    }
                }
                stage('choreography-payment-service') {
                    when { expression { return env.BUILD_CHOREOGRAPHY_PAYMENT == 'true' } }
                    steps {
                        buildAndPushService('choreography-saga/payment-service', 'choreography-payment-service')
                    }
                }
                stage('choreography-inventory-service') {
                    when { expression { return env.BUILD_CHOREOGRAPHY_INVENTORY == 'true' } }
                    steps {
                        buildAndPushService('choreography-saga/inventory-service', 'choreography-inventory-service')
                    }
                }
                stage('choreography-shipping-service') {
                    when { expression { return env.BUILD_CHOREOGRAPHY_SHIPPING == 'true' } }
                    steps {
                        buildAndPushService('choreography-saga/shipping-service', 'choreography-shipping-service')
                    }
                }
                stage('orchestration-order-service') {
                    when { expression { return env.BUILD_ORCHESTRATION_ORDER == 'true' } }
                    steps {
                        buildAndPushService('orchestration-saga/order-service', 'orchestration-order-service')
                    }
                }
                stage('orchestration-payment-service') {
                    when { expression { return env.BUILD_ORCHESTRATION_PAYMENT == 'true' } }
                    steps {
                        buildAndPushService('orchestration-saga/payment-service', 'orchestration-payment-service')
                    }
                }
                stage('orchestration-inventory-service') {
                    when { expression { return env.BUILD_ORCHESTRATION_INVENTORY == 'true' } }
                    steps {
                        buildAndPushService('orchestration-saga/inventory-service', 'orchestration-inventory-service')
                    }
                }
                stage('orchestration-shipping-service') {
                    when { expression { return env.BUILD_ORCHESTRATION_SHIPPING == 'true' } }
                    steps {
                        buildAndPushService('orchestration-saga/shipping-service', 'orchestration-shipping-service')
                    }
                }
            }
        }
    }

    post {
        always {
            sh "docker system prune -af --volumes || true"
            cleanWs()
        }
        success {
            script {
                def serviceCount = [
                    env.BUILD_CHOREOGRAPHY_ORDER, env.BUILD_CHOREOGRAPHY_PAYMENT,
                    env.BUILD_CHOREOGRAPHY_INVENTORY, env.BUILD_CHOREOGRAPHY_SHIPPING,
                    env.BUILD_ORCHESTRATION_ORDER, env.BUILD_ORCHESTRATION_PAYMENT,
                    env.BUILD_ORCHESTRATION_INVENTORY, env.BUILD_ORCHESTRATION_SHIPPING
                ].count { it == 'true' }

                discordSend description: """Build Succeeded: ${env.JOB_NAME} #${env.BUILD_NUMBER}

**Branch:** ${env.BRANCH_NAME}
**Commit:** ${env.SHORT_SHA}
**Services Built:** ${serviceCount}
**Build Scope:** ${params.BUILD_SCOPE}

CC: ${env.MENTION}""",
                    footer: "Built by Jenkins",
                    link: env.BUILD_URL,
                    result: 'SUCCESS',
                    title: "✅ Build Succeeded",
                    webhookURL: env.DISCORD_WEBHOOK
            }
        }
        failure {
            discordSend description: """Build Failed: ${env.JOB_NAME} #${env.BUILD_NUMBER}

**Branch:** ${env.BRANCH_NAME}
**Commit:** ${env.SHORT_SHA}
**Build Scope:** ${params.BUILD_SCOPE}

Check console output for details.
CC: ${env.MENTION}""",
                footer: "Jenkins CI",
                link: env.BUILD_URL,
                result: 'FAILURE',
                title: "❌ Build Failed",
                webhookURL: env.DISCORD_WEBHOOK
        }
    }
}

def buildAndPushService(String svcPath, String svcName) {
    def imageTag = "${IMAGE_PREFIX}/${svcName}:sha-${env.SHORT_SHA}"
    def latestTag = "${IMAGE_PREFIX}/${svcName}:latest"
    def branchTag = "${IMAGE_PREFIX}/${svcName}:${env.BRANCH_NAME.replaceAll('/', '-')}"

    echo "=== Building ${svcName} ==="

    timeout(time: 20, unit: 'MINUTES') {
        sh """
            mvn spring-boot:build-image -DskipTests --batch-mode \
            -pl ${svcPath} \
            -Dspring-boot.build-image.imageName=${imageTag} \
            -Dspring-boot.build-image.environment.BP_NATIVE_IMAGE=true \
            -Dspring-boot.build-image.cleanCache=true
        """
    }

    // Push with retry logic
    retry(3) {
        sh "docker push ${imageTag}"
    }

    // Tag and push latest (only on main branch)
    if (env.BRANCH_NAME == 'main') {
        sh "docker tag ${imageTag} ${latestTag}"
        retry(3) {
            sh "docker push ${latestTag}"
        }
    }

    // Tag with branch name
    sh "docker tag ${imageTag} ${branchTag}"
    retry(3) {
        sh "docker push ${branchTag}"
    }

    echo "=== Successfully published ${svcName} ==="
}