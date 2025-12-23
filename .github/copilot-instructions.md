# Orglang

This a visual programming language to support organization creation and management.

## Repository structure

- `app`: Application packages which are compositions of aggregate roots
- `root`: Aggregate root entities which have domain services
- `db`: Database schema migrations
    - `postgres`: PostgreSQL specific migrations
- `internal`: Reusable entities which have no domain services
- `lib`: Reusable abstract data types (ADT)
- `orch`: Local orchestration harness 
    - `task`: Task (build tool) harness implementation
- `proto`: Prototypes
- `stack`: Stacks
- `test`: End-to-end tests and harness

## Aggregate root structure

### Framework agnostic

- `msg.go`: Pure message exchange logic
    - Network specific DTO's (borderline models)
    - Message validation harness
    - Message to domain mapping and vice versa
- `core.go`: Pure business domain logic
    - Domain models (core models)
    - API interfaces (primary ports)
    - Domain services (API implementations)
    - Repository interfaces (secondary ports)
- `data.go`: Pure data persistence logic
    - Storage specific DTO's (borderline models)
    - Domain to data mapping and vice versa

### Framework specific

- `msg_echo.go`: Echo (web framework) specific controllers (primary adapters)
- `data_pgx.go`: pgx (PostgreSQL Driver and Toolkit) specific repository iplementations (secondary adapters)
- `sdk_resty.go`: Resty (HTTP client) specific API implementations (secondary adapters)
- `di_fx.go`: Fx (dependency injection system) specific component definitions
