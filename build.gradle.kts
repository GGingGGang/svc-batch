plugins {
    java
    id("org.springframework.boot") version "3.4.2"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "cloud.ggang"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

// 결정적 jar 이름: build/libs/svc-batch.jar (Dockerfile 이 이 경로를 집는다)
tasks.bootJar {
    archiveFileName = "svc-batch.jar"
}

// plain jar 비활성 — build/libs 에 bootJar 산출물 하나만
tasks.jar {
    enabled = false
}

tasks.withType<Test> {
    useJUnitPlatform()
}
