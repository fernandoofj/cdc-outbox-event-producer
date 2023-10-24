package shop.inventa.pg2sns4k.console

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import io.awspring.cloud.messaging.core.NotificationMessagingTemplate
import shop.inventa.pg2sns4k.jackson.MappingJackson2MessageConverterCustom
import shop.inventa.pg2sns4k.replication.config.PostgresConfiguration
import shop.inventa.pg2sns4k.replication.config.ReplicationConfiguration
import shop.inventa.pg2sns4k.workflow.SlotReaderSNSProducer
import java.io.FileInputStream
import java.util.Properties

@Suppress("TooGenericExceptionCaught")
class CommandLineRunnerTest : Runnable {

    override fun run() {
        val properties = Properties()

        val inputStream = FileInputStream("src/test/resources/application.properties")
        properties.load(inputStream)

        val host = properties.getProperty("datasource.host")
        val port = properties.getProperty("datasource.port")
        val username = properties.getProperty("datasource.username")
        val password = properties.getProperty("datasource.password")
        val database = properties.getProperty("datasource.database")
        val slotName = properties.getProperty("datasource.replication.slot")
        val awsAccessKey = properties.getProperty("cloud.aws.credentials.access-key")
        val secretKey = properties.getProperty("cloud.aws.credentials.secret-key")
        val region = properties.getProperty("cloud.aws.region.static")
        val localStackUrl = properties.getProperty("cloud.aws.localstack.url")

        val notificationMessagingTemplate = NotificationMessagingTemplate(
            AmazonSNSClientBuilder
                .standard()
                .withEndpointConfiguration(
                    AwsClientBuilder.EndpointConfiguration(localStackUrl, region)
                )
                .withCredentials(
                    AWSStaticCredentialsProvider(
                        BasicAWSCredentials(awsAccessKey, secretKey)
                    )
                )
                .build()
        ).apply {
            messageConverter = MappingJackson2MessageConverterCustom(ObjectMapper()).jackson2MessageConverter()
        }

        SlotReaderSNSProducer(
            PostgresConfiguration(host, port, database, username, password),
            ReplicationConfiguration(slotName),
            shop.inventa.pg2sns4k.aws.sns.SNSTransactionalProducer(notificationMessagingTemplate)
        ).startStreaming()
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            CommandLineRunnerTest().run()
        }
    }
}
