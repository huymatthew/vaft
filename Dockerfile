# ============================================
# Stage 1: Build project bằng Maven
# ============================================
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

# Copy pom.xml trước để tận dụng Docker layer cache
# (nếu pom.xml không đổi, không cần tải lại dependency mỗi lần build)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy toàn bộ source code (bao gồm src/main/proto)
COPY src ./src

# Build project: compile + generate code từ .proto + đóng gói jar kèm dependency
RUN mvn clean package -DskipTests

# ============================================
# Stage 2: Runtime image (nhẹ hơn, không có Maven)
# ============================================
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=build /app/target/vaft-1.0-jar-with-dependencies.jar app.jar

COPY src/main/resources/cluster.yaml /app/cluster.yaml

EXPOSE 9001


ENTRYPOINT ["java", "-jar", "app.jar"]
CMD ["--nodeId=1", "--config=/app/cluster.yaml"]