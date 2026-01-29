pipeline {
    agent none  // Global agent is none to allow parallel queuing

    parameters {
        string(name: 'BUILD_SCOPE', defaultValue: 'all', description: 'all, choreography-only, orchestration-only, changed-only')
        booleanParam(name: 'FORCE_REBUILD', defaultValue: false, description: 'Force a rebuild of all services in the selected scope')
        booleanParam(name: 'RUN_BENCHMARK', defaultValue: false, description: 'Run thesis benchmark after successful build')
        choice(name: 'BENCHMARK_PROFILE', choices: ['quick', 'thesis-baseline', 'thesis-stress'], description: 'Benchmark profile (only used if RUN_BENCHMARK is true)')
    }

    triggers {
        githubPush()
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timeout(time: 120, unit: 'MINUTES')
        timestamps()
        disableConcurrentBuilds()
    }

    environment {
        REGISTRY = 'ghcr.io'
        IMAGE_PREFIX = "ghcr.io/elskow/saga-pattern"
        GHCR_CREDS = credentials('github-ghcr-creds')
        DISCORD_WEBHOOK = credentials('discord-webhook-url')
        MAVEN_OPTS = '-Xmx4g -XX:+UseG1GC'
        FAILED_SERVICES = ''
        MENTION = '<@594544493869531292>'
        // Disabled BuildKit to fix missing plugin error
        DOCKER_BUILDKIT = '0'
    }

    stages {
        // STAGE 1: PLANNING (Uses 1 Executor)
        stage('Plan & Notify') {
            agent any
            steps {
                checkout scm
                script {
                    env.SHORT_SHA = sh(script: 'git rev-parse --short HEAD', returnStdout: true).trim()
                    env.BRANCH_NAME = sh(script: 'git rev-parse --abbrev-ref HEAD', returnStdout: true).trim()

                    // --- LOGIC TO DECIDE WHAT TO BUILD ---
                    def buildAll = params.BUILD_SCOPE == 'all' || params.FORCE_REBUILD
                    def buildChoreo = buildAll || params.BUILD_SCOPE == 'choreography-only'
                    def buildOrch = buildAll || params.BUILD_SCOPE == 'orchestration-only'

                    env.BUILD_CHOREOGRAPHY_ORDER = buildChoreo ? 'true' : 'false'
                    env.BUILD_CHOREOGRAPHY_PAYMENT = buildChoreo ? 'true' : 'false'
                    env.BUILD_CHOREOGRAPHY_INVENTORY = buildChoreo ? 'true' : 'false'
                    env.BUILD_CHOREOGRAPHY_SHIPPING = buildChoreo ? 'true' : 'false'
                    env.BUILD_ORCHESTRATION_ORDER = buildOrch ? 'true' : 'false'
                    env.BUILD_ORCHESTRATION_PAYMENT = buildOrch ? 'true' : 'false'
                    env.BUILD_ORCHESTRATION_INVENTORY = buildOrch ? 'true' : 'false'
                    env.BUILD_ORCHESTRATION_SHIPPING = buildOrch ? 'true' : 'false'

                    // --- NOTIFICATION ---
                    def serviceCount = [
                        env.BUILD_CHOREOGRAPHY_ORDER, env.BUILD_CHOREOGRAPHY_PAYMENT,
                        env.BUILD_CHOREOGRAPHY_INVENTORY, env.BUILD_CHOREOGRAPHY_SHIPPING,
                        env.BUILD_ORCHESTRATION_ORDER, env.BUILD_ORCHESTRATION_PAYMENT,
                        env.BUILD_ORCHESTRATION_INVENTORY, env.BUILD_ORCHESTRATION_SHIPPING
                    ].count { it == 'true' }

                    def benchmarkInfo = params.RUN_BENCHMARK ? "\n**Benchmark:** ${params.BENCHMARK_PROFILE} (after build)" : ""

                    discordSend description: """Build Started: ${env.JOB_NAME} #${env.BUILD_NUMBER}
**Branch:** ${env.BRANCH_NAME}
**Services:** ${serviceCount}
**Executors:** 2 (Queued Mode)${benchmarkInfo}""",
                        footer: "Jenkins CI",
                        link: env.BUILD_URL,
                        result: 'SUCCESS',
                        title: "🚀 Build Started",
                        webhookURL: env.DISCORD_WEBHOOK
                }
            }
        }

        // STAGE 2: PARALLEL EXECUTION (Queuing enforced here)
        stage('Build Services') {
            parallel {
                stage('Choreography Order') {
                    agent any // <--- New Agent = New Slot in Queue
                    when { expression { return env.BUILD_CHOREOGRAPHY_ORDER == 'true' } }
                    steps {
                        buildAndPushService('choreography-saga/order-service', 'choreography-order-service')
                    }
                }
                stage('Choreography Payment') {
                    agent any
                    when { expression { return env.BUILD_CHOREOGRAPHY_PAYMENT == 'true' } }
                    steps {
                        buildAndPushService('choreography-saga/payment-service', 'choreography-payment-service')
                    }
                }
                stage('Choreography Inventory') {
                    agent any
                    when { expression { return env.BUILD_CHOREOGRAPHY_INVENTORY == 'true' } }
                    steps {
                        buildAndPushService('choreography-saga/inventory-service', 'choreography-inventory-service')
                    }
                }
                stage('Choreography Shipping') {
                    agent any
                    when { expression { return env.BUILD_CHOREOGRAPHY_SHIPPING == 'true' } }
                    steps {
                        buildAndPushService('choreography-saga/shipping-service', 'choreography-shipping-service')
                    }
                }
                stage('Orchestration Order') {
                    agent any
                    when { expression { return env.BUILD_ORCHESTRATION_ORDER == 'true' } }
                    steps {
                        buildAndPushService('orchestration-saga/order-service', 'orchestration-order-service')
                    }
                }
                stage('Orchestration Payment') {
                    agent any
                    when { expression { return env.BUILD_ORCHESTRATION_PAYMENT == 'true' } }
                    steps {
                        buildAndPushService('orchestration-saga/payment-service', 'orchestration-payment-service')
                    }
                }
                stage('Orchestration Inventory') {
                    agent any
                    when { expression { return env.BUILD_ORCHESTRATION_INVENTORY == 'true' } }
                    steps {
                        buildAndPushService('orchestration-saga/inventory-service', 'orchestration-inventory-service')
                    }
                }
                stage('Orchestration Shipping') {
                    agent any
                    when { expression { return env.BUILD_ORCHESTRATION_SHIPPING == 'true' } }
                    steps {
                        buildAndPushService('orchestration-saga/shipping-service', 'orchestration-shipping-service')
                    }
                }
            }
        }

        // STAGE 3: TRIGGER BENCHMARK (Optional)
        stage('Trigger Benchmark') {
            agent any
            when {
                expression { return params.RUN_BENCHMARK == true }
            }
            steps {
                script {
                    echo "Triggering thesis benchmark pipeline..."
                    
                    // Trigger the benchmark pipeline (infrastructure must be pre-provisioned)
                    build job: 'saga-pattern-benchmark',
                        wait: false,  // Don't wait for benchmark to complete
                        parameters: [
                            choice(name: 'PROFILE', value: params.BENCHMARK_PROFILE),
                            choice(name: 'SIMULATION', value: 'SustainedMixedSimulation'),
                            booleanParam(name: 'RUN_CHOREOGRAPHY', value: true),
                            booleanParam(name: 'RUN_ORCHESTRATION', value: true),
                            string(name: 'WARMUP_REQUESTS', value: '100'),
                            string(name: 'COOLDOWN_SECONDS', value: '30')
                        ]
                    
                    discordSend description: """Benchmark Triggered: saga-pattern-benchmark
**Profile:** ${params.BENCHMARK_PROFILE}
**Triggered by:** Build #${env.BUILD_NUMBER}""",
                        footer: "Jenkins CI",
                        link: env.BUILD_URL,
                        result: 'SUCCESS',
                        title: "🧪 Benchmark Triggered",
                        webhookURL: env.DISCORD_WEBHOOK
                }
            }
        }
    }

    post {
        // Global post block runs on Flyweight (Controller) because agent is none.
        // We removed 'cleanWs()' from here because there is no workspace to clean globally.
        // Cleanup happens inside buildAndPushService.

        success {
            // Removed 'node' wrapper - discordSend handles this natively
            discordSend description: """Build Succeeded: ${env.JOB_NAME} #${env.BUILD_NUMBER}
**Branch:** ${env.BRANCH_NAME}""",
                footer: "Built by Jenkins",
                link: env.BUILD_URL,
                result: 'SUCCESS',
                title: "✅ Build Succeeded",
                webhookURL: env.DISCORD_WEBHOOK
        }
        failure {
            discordSend description: """Build Failed: ${env.JOB_NAME} #${env.BUILD_NUMBER}
Check console output for details.""",
                footer: "Jenkins CI",
                link: env.BUILD_URL,
                result: 'FAILURE',
                title: "❌ Build Failed",
                webhookURL: env.DISCORD_WEBHOOK
        }
    }
}

def buildAndPushService(String svcPath, String svcName) {
    try {
        // IMPORTANT: Since we are on a new agent, we MUST checkout code again
        checkout scm

        // Also need to login again because this is a fresh executor session
        withCredentials([usernamePassword(credentialsId: 'github-ghcr-creds', passwordVariable: 'GHCR_PSW', usernameVariable: 'GHCR_USR')]) {
            sh "echo ${GHCR_PSW} | docker login ghcr.io -u ${GHCR_USR} --password-stdin"
        }

        def imageTag = "${IMAGE_PREFIX}/${svcName}:sha-${env.SHORT_SHA}"
        def latestTag = "${IMAGE_PREFIX}/${svcName}:latest"
        def branchTag = "${IMAGE_PREFIX}/${svcName}:${env.BRANCH_NAME.replaceAll('/', '-')}"

        def pattern = svcPath.split('/')[0].replace('-saga', '')
        def servicePart = svcPath.split('/')[1]
        def artifactId = "${pattern}-${servicePart}"

        echo "=== Building ${svcName} (artifactId: ${artifactId}) ==="

        timeout(time: 45, unit: 'MINUTES') {
            sh """
                docker build \
                    --build-arg SERVICE_PATH=${svcPath} \
                    --build-arg ARTIFACT_ID=${artifactId} \
                    -t ${imageTag} \
                    -f Dockerfile \
                    .
            """
        }

        retry(3) { sh "docker push ${imageTag}" }

        if (env.BRANCH_NAME == 'main') {
            sh "docker tag ${imageTag} ${latestTag}"
            retry(3) { sh "docker push ${latestTag}" }
        }

        sh "docker tag ${imageTag} ${branchTag}"
        retry(3) { sh "docker push ${branchTag}" }

        // Cleanup local images to save disk space
        sh "docker rmi ${imageTag} || true"

    } finally {
        // Clean workspace after this specific parallel branch finishes
        cleanWs()
    }
}
