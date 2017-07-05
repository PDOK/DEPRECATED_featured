FROM openjdk:8-jre

ARG version=unknown
LABEL nl.pdok.application="featured"
LABEL nl.pdok.version=$version

COPY artifacts/featured-cli.jar /opt
COPY run.sh /opt
RUN chmod 0500 /opt/run.sh
WORKDIR /opt
ENTRYPOINT ["./run.sh", "featured-cli.jar"]
