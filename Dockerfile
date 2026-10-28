FROM maven:3.9.9-eclipse-temurin-21
WORKDIR /app
COPY target/concurrency-study-1.0-SNAPSHOT.jar /app/study.jar
ENV JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=2 -Xms256m -Xmx1g"
ENTRYPOINT ["java", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-jar", "/app/study.jar"]
