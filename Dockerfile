# Multi-stage build for Ad Analytics Pipeline
# Stage 1: Build with Maven
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn package -DskipTests -B

# Stage 2: Lightweight runtime
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

# Copy built JAR
COPY --from=build /app/target/ad-analytics-pipeline.jar ./app.jar

# Create directories
RUN mkdir -p /app/data /app/results

# Set memory limits
ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Expose REST API port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s \
    CMD wget -qO- http://localhost:8080/actuator/health || exit 1

# Default: run in API mode
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar $@"]
