FROM eclipse-temurin:21-jre
WORKDIR /app
COPY recon-api/target/recon-api-*.jar fat.jar
# Extract nested JARs to plain files so the JVM loads from the filesystem
# instead of decompressing entries inside a nested-JAR classloader.
# This eliminates ~3 min of startup overhead caused by the 90 MB ONNX
# embedding model JAR being indexed on every boot.
RUN java -Djarmode=tools -jar fat.jar extract --destination /tmp/extracted && \
    mv /tmp/extracted/lib . && \
    mv /tmp/extracted/fat.jar app.jar && \
    rm -rf /tmp/extracted fat.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
