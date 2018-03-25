package io.thenatureofsoftware.amazonaws

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import java.io.File
import java.util.concurrent.TimeUnit


fun String.runCommand() = ProcessBuilder(*split(" ").toTypedArray())
            .directory(File(DynamoDBLocalRemoteCtrl.dynamoDbLocalHome()))
            .redirectOutput(ProcessBuilder.Redirect.INHERIT)
            .redirectError(ProcessBuilder.Redirect.INHERIT)
            .start()

class DynamoDBLocalRemoteCtrl {

    companion object {
        var process : Process? = null

        fun dynamoDbLocalHome() = System.getenv("DYNAMODB_LOCAL_HOME") ?: ""

        fun start(config: Config): Handle {
            if (process != null) stop()

            val runPort = "-port ${config.port}"
            val runMode = if (config.dbPath == "") "-inMemory" else "-dbPath ${config.dbPath}"
            val shared = if (config.shared) "-sharedDb" else ""
            val delayTransientStatuses = if (config.delayTransientStatuses) "-delayTransientStatuses" else ""

            val startProcess = "java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar $runPort $runMode $shared $delayTransientStatuses"
                    .trim()
                    .runCommand()

            process = startProcess
            return Handle(startProcess, createAmazonDynamoDB(config.port))
        }

        fun createAmazonDynamoDB(port: Int) = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://localhost:$port", ""))
                .withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials("", "")))
                .build()

        fun stop(): Boolean {
            return stop(process)
        }

        fun stop(handle: Handle): Boolean {
            return stop(handle.process)
        }

        private fun stop(dynamoDBProcess: Process?): Boolean {
            if (dynamoDBProcess == null) return false
            dynamoDBProcess.destroyForcibly().waitFor(60, TimeUnit.SECONDS)
            return true
        }
    }
}

data class Config(
        val port: Int = 8000,
        val shared: Boolean = true,
        val dbPath: String = "",
        val delayTransientStatuses: Boolean = false)

data class Handle(val process: Process, val amazonDynamoDB: AmazonDynamoDB)