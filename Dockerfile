FROM rust:alpine AS builder
#FROM rustlang/rust:nightly-alpine AS builder

RUN apk add --no-cache musl-dev unixodbc-static

WORKDIR /src/odbc2parquet
COPY . .
RUN cargo build --release

FROM scratch AS runner

COPY --from=builder /src/odbc2parquet/target/release/odbc2parquet /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/odbc2parquet"]
