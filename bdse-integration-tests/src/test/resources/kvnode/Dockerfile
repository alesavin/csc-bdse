# https://hub.docker.com/_/java/
FROM openjdk:8-jdk-alpine
MAINTAINER Alexander Savin <alesavin@gmail.com>
VOLUME /tmp
ADD target/bdse-kvnode-0.0.2-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/app.jar", "ru.csc.bdse.Application"]