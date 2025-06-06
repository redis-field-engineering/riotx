# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RIOT-X is an enterprise extension of the Redis Input/Output Tool (RIOT) designed for comprehensive Redis data management. It provides ETL capabilities for various data sources including files (CSV, JSON, XML, Parquet), databases, data generators, and Redis-to-Redis replication.

## Essential Build Commands

### Development Workflow
```bash
# Build entire project
./gradlew build

# Run tests for all modules
./gradlew test

# Run tests for specific module
./gradlew :riotx:test
./gradlew :riot-core:test

# Clean and rebuild
./gradlew clean build
```

### Running the Application

```bash
# Quick run via convenience script (builds and runs)
./riotx [command] [options]

# Run via Gradle during development
./gradlew :riotx:run --args="[command] [options]"

# Install distribution then run
./gradlew :riotx:installDist
./plugins/riotx/build/install/riotx/bin/riotx [command] [options]
```

### Key RIOT-X Commands
```bash
riotx file-import    # Import data from files
riotx file-export    # Export data to files  
riotx db-import      # Import from databases
riotx db-export      # Export to databases
riotx replicate      # Redis-to-Redis replication
riotx generate       # Generate fake/test data
riotx compare        # Compare Redis instances
riotx ping           # Test Redis connectivity
```

## Architecture

### Multi-Module Structure

**Core Modules** (`/core/`)
- `riot-core`: Core utilities, expressions, processors, and shared functionality
- `riot-db`: JDBC database connectivity and operations (including Snowflake)
- `riot-faker`: Fake data generation using DataFaker library
- `riot-file`: File I/O operations for CSV, JSON, XML formats
- `riot-parquet`: Apache Parquet file format support
- `riot-rdi`: Redis Data Integration features

**Application Module** (`/plugins/`)
- `riotx`: Main CLI application using PicoCLI for command structure

**Spring Batch Infrastructure** (`/spring-batch/`)
- `spring-batch-redis-core`: Redis ItemReaders/ItemWriters and core batch functionality
- `spring-batch-redis-infrastructure`: Flushing steps, metrics, and batch infrastructure
- `spring-batch-memcached`: Memcached integration support
- `spring-batch-redis-test`: Shared test utilities for Redis batch operations
- `spring-batch-resource`: Resource management utilities

### Key Architectural Patterns

- **Command Pattern**: Uses PicoCLI for structured CLI with subcommands
- **Spring Batch**: Leverages Spring Batch for robust, scalable data processing with chunk-based processing
- **Modular Design**: Clean separation between data sources, processing logic, and CLI interface
- **Backpressure Handling**: Built-in backpressure management for high-throughput operations

## Development Environment

- **Java 17+** required
- **Spring Boot 3.4.5** with Spring Batch for ETL processing
- **Lettuce** as the Redis client library
- **TestContainers** for integration testing with real Redis instances
- **Lombok** for reducing boilerplate code

## Testing Strategy

- Unit tests with JUnit 5
- Integration tests using TestContainers with real Redis/Memcached instances
- Extensive test resources for various data formats in `src/test/resources/`
- Coverage reporting with JaCoCo
- Test files follow pattern: `[feature]-[operation]` (e.g., `file-import-csv`, `replicate-live`)

## Key Implementation Notes

- All commands extend from `AbstractJobCommand` which provides Spring Batch integration
- Redis operations use `RedisArgs` for connection configuration with environment variable support
- File operations support cloud storage (S3, Google Cloud Storage) via protocol resolvers
- Backpressure and throttling are configurable via `StepArgs` and `FlushingStepArgs`
- Expression processing uses Spring Expression Language (SpEL) for data transformation
- The main entry point is `com.redis.riot.Riotx` in the `riotx` module