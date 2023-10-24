FROM openjdk:17-slim

RUN mkdir /app

#COPY /build/libs/kotlin-postgres-cdc-to-sns-module.jar /app/

#ENTRYPOINT [ "java", "-Dlogging.pattern.console=%d{yyyy-MM-dd HH:mm:ss} - %logger{36} - %msg traceID=%X{trace_id} %n", "-jar", "/app/kotlin-postgres-cdc-to-sns-module.jar" ]

ENTRYPOINT [ "java", "-Dlogging.pattern.console=%d{yyyy-MM-dd HH:mm:ss} - %logger{36} - %msg traceID=%X{trace_id} %n" ]
