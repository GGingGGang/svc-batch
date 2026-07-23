plugins {
    java
    checkstyle
    id("org.springframework.boot") version "3.4.2"
    id("io.spring.dependency-management") version "1.1.7"
    id("com.diffplug.spotless") version "8.8.0"
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

// Spring Boot 3.4.2 관리 버전(10.1.34)에 CVE 대응 등으로 패치 필요 시 override — Tomcat 11.0.x(Jakarta
// EE 11/Servlet 6.1)는 Spring Boot 4 부터 지원이라 여기선 10.1.x 라인 안에서만 올린다. tomcat.version
// 프로퍼티 하나로 tomcat-embed-core/-el/-websocket 이 함께 맞춰짐(Spring Boot BOM 표준 override 방식).
extra["tomcat.version"] = "10.1.55"

// Spring Boot 3.4.x 의 dependency-management 가 shedlock 을 관리하지 않으므로 명시 고정.
// shedlock 7.x 부터는 Spring Framework 7(Boot 4) 대상이라 6.x 최신판 사용 (Spring 6.2.x 호환).
val shedlockVersion = "6.10.0"

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-batch")
    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("io.nats:jnats:2.26.0")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-mysql")
    implementation("net.javacrumbs.shedlock:shedlock-spring:$shedlockVersion")
    implementation("net.javacrumbs.shedlock:shedlock-provider-redis-spring:$shedlockVersion")
    runtimeOnly("com.mysql:mysql-connector-j")
    runtimeOnly("io.micrometer:micrometer-registry-prometheus")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.batch:spring-batch-test")
    // testcontainers 버전은 spring-boot-dependencies 가 관리 (testcontainers-bom import) — 별도 버전 고정 불필요.
    // NATS 는 org.testcontainers 공식 모듈이 없어 GenericContainer + 공식 nats 이미지로 직접 기동(운영과 동일 2.14.2-alpine).
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:mysql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

checkstyle {
    toolVersion = "10.26.1"
    isIgnoreFailures = true
}

spotless {
    java {
        target("src/**/*.java")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
    kotlinGradle {
        target("*.gradle.kts")
        trimTrailingWhitespace()
        endWithNewline()
    }
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
