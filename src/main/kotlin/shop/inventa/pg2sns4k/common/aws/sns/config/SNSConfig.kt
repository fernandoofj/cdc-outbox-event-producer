package shop.inventa.pg2sns4k.common.aws.sns.config

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.sns.AmazonSNS
import com.amazonaws.services.sns.AmazonSNSClientBuilder
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile
import shop.inventa.pg2sns4k.common.aws.parameters.AWSParameters

@Configuration
@Profile("!local")
class SNSConfig(
    private val awsParameters: AWSParameters,
) {
    @Bean
    @Primary
    fun amazonSNS(): AmazonSNS =
        AmazonSNSClientBuilder
            .standard()
            .withCredentials(DefaultAWSCredentialsProviderChain())
            .withRegion(awsParameters.region)
            .build()
}
