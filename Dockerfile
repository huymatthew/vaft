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

# Copy jar đã build từ stage 1
# Tên jar khớp với groupId/artifactId/version trong pom.xml (vaft-1.0-jar-with-dependencies.jar)
COPY --from=build /app/target/vaft-1.0-jar-with-dependencies.jar app.jar

# Copy config mặc định (có thể override khi chạy container bằng volume mount)
# LƯU Ý: bỏ comment dòng dưới sau khi đã tạo file src/main/resources/cluster.yaml
# COPY src/main/resources/cluster.yaml /app/cluster.yaml

# Port gRPC lắng nghe (khớp với config port của node)
EXPOSE 9001

# nodeId truyền vào lúc chạy container qua biến môi trường hoặc argument
ENTRYPOINT ["java", "-jar", "app.jar"]
CMD ["--nodeId=1", "--config=/app/cluster.yaml"]