ARG RUST_IMAGE=docker.io/library/rust:1.56.0
ARG RUNTIME_IMAGE=gcr.io/distroless/cc

# Builds the engula binary.
FROM $RUST_IMAGE as build
ARG TARGETARCH
WORKDIR /build
COPY . /build/
RUN --mount=type=cache,target=target \
	--mount=type=cache,from=rust:1.56.0,source=/usr/local/cargo,target=/usr/local/cargo \
	cargo build --locked --target=x86_64-unknown-linux-gnu --release --package=engula && \
	mv target/x86_64-unknown-linux-gnu/release/engula /tmp/

# Creates a minimal runtime image with the engula binary.
FROM $RUNTIME_IMAGE
COPY --from=build /tmp/engula /bin/
ENTRYPOINT ["/bin/engula"]
