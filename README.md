# Tiny event sourcing library

## Installation

To add library to your project put the following dependency in your `pom.xml`:
```
<dependency>
    <groupId>ru.quipy</groupId>
    <artifactId>tiny-event-sourcing-lib</artifactId>
    <version>${library.version}/version>
</dependency>
```

Also you have to configure the `github` maven repository. You can either include it to your `settings.xml` or just put the following lines to your `pom.xml`: 

```
<repository>
    <id>github</id>
    <url>https://andrsuh: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@maven.pkg.github.com/andrsuh/tiny-event-sourcing</url>
</repository>
```