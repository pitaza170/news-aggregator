FROM openjdk:17-jdk-slim
COPY target/news-aggregator-*.jar news-aggregator.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "news-aggregator.jar"]

