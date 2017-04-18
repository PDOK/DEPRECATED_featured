FROM openjdk:8-jre

ARG version=unknown
LABEL nl.pdok.application="featured"
LABEL nl.pdok.version=$version

COPY artifacts/featured-cli.jar /opt
WORKDIR /opt
ENTRYPOINT ["java", "-jar", "featured-cli.jar"]
