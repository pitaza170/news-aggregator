FROM openjdk:17-jdk-slim
ADD target/news-aggregator.jar news-aggregator.jar
ENTRYPOINT ["java", "-jar","news-aggregator.jar"]
EXPOSE 8080