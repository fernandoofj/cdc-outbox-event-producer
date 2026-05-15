package br.com.fltech.cdc.outbox.publisher.aws.common

data class AWSParamaters(
    val region: String,
    val awsAccessKey: String,
    val awsSecretKey: String,
    val localstackUrl: String
)
