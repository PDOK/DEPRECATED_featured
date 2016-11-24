FROM openjdk:8-jre

ARG version=unknown
LABEL nl.pdok.application="featured"
LABEL nl.pdok.version=$version

COPY artifacts/featured-web.jar /opt
WORKDIR /opt
CMD ["java", "-jar", "featured-web.jar"]