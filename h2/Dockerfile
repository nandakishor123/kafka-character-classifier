# Standalone H2 server so every worker instance shares one database.
# Version matches the H2 driver managed by Spring Boot 3.5.
FROM eclipse-temurin:21-jre
ADD https://repo1.maven.org/maven2/com/h2database/h2/2.3.232/h2-2.3.232.jar /opt/h2.jar
EXPOSE 9092 8082
# -ifNotExists lets clients create the DB remotely: fine for local learning, never for production.
CMD ["java", "-cp", "/opt/h2.jar", "org.h2.tools.Server", \
     "-tcp", "-tcpAllowOthers", "-tcpPort", "9092", \
     "-web", "-webAllowOthers", "-webPort", "8082", \
     "-ifNotExists", "-baseDir", "/data"]
