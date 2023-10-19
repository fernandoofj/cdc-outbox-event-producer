package shop.inventa.pg2sns4k.common.aws.sns.config

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile
import shop.inventa.pg2sns4k.common.aws.parameters.AWSParameters

@Configuration
@Profile("local")
class SNSLocalConfig(
    private val awsParameters: AWSParameters,
) {

    @Bean
    @Primary
    fun amazonSNSLocal(): AmazonSNS =
        AmazonSNSClientBuilder
            .standard()
            .withEndpointConfiguration(
                AwsClientBuilder.EndpointConfiguration(awsParameters.localstackUrl, awsParameters.region)
            )
            .withCredentials(
                AWSStaticCredentialsProvider(
                    BasicAWSCredentials(awsParameters.awsAccessKey, awsParameters.awsSecretKey)
                )
            )
            .build()
}
