package shop.inventa.pg2sns4k.common.aws.parameters

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class AWSParameters(
    @Value("\${cloud.aws.region.static}")
    val region: String,

    @Value("\${cloud.aws.credentials.access-key:}")
    val awsAccessKey: String,

    @Value("\${cloud.aws.credentials.secret-key:}")
    val awsSecretKey: String,

    @Value("\${cloud.aws.localstack.url:http://localhost:4566}")
    val localstackUrl: String
)
