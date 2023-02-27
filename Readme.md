# Fork of 4mc - Four More Compression

## About
Used by [util-cofig](https://github.com/sewichi/util-config).

## Versions
Version 2.0.1 is deployed as a jar to artifacts.placed.com `thirdparty` repo [here](https://artifacts.placed.com/nexus/content/repositories/s3-thirdparty-proxy/com/hadoop/fourmc/sewichi-hadoop-4mc/2.0.1/).

Version 3.0.0 is deployed as a jar to artifacts.placed.com `release` repo [here](https://artifacts.placed.com/nexus/content/repositories/s3-release-proxy/com/fing/fourmc/hadoop-4mc/3.0.0/).

Version 3.0.1 (supports arm64/aarch64) is deployed as a jar to artifacts.placed.com `release` repo [here](https://artifacts.placed.com/nexus/content/repositories/s3-release-proxy/com/fing/fourmc/hadoop-4mc/3.0.1/).

## CI/CD
Currently, CI/CD for this repo is not configured.
To deploy a new version of this library, follow [this doc](https://github.com/sewichi/engineering-guides/blob/master/new-hire/setup.md) for setting up your environment, bump the artifact's version [here](https://github.com/sewichi/4mc/blob/5f2307738868d42c10cff3791ef83722134b63a4/java/hadoop-4mc/pom.xml#L23), then run following commands:
```
cd java/hadoop-4mc
mvn clean package
```
Check that jar was generated (for example, at location `4mc/java/hadoop-4mc/target/hadoop-4mc-3.0.1.jar`), then deploy to release repo:
```
mvn deploy
```
