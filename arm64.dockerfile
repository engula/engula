ARG RUST_IMAGE=docker.io/library/rust:1.56.0
ARG RUNTIME_IMAGE=gcr.io/distroless/cc

FROM $RUST_IMAGE as build
RUN apt-get update && \
	apt-get install -y --no-install-recommends g++-aarch64-linux-gnu libc6-dev-arm64-cross && \
	apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/ && \
	rustup target add aarch64-unknown-linux-gnu
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
WORKDIR /build
COPY . /build/

RUN --mount=type=cache,target=target \
	--mount=type=cache,from=rust:1.56.0,source=/usr/local/cargo,target=/usr/local/cargo \
	cargo build --locked --release --target=aarch64-unknown-linux-gnu \
	--package=engula --no-default-features && \
	mv target/aarch64-unknown-linux-gnu/release/engula /tmp/

FROM --platform=linux/arm64 $RUNTIME_IMAGE
COPY --from=build /tmp/engula /bin/
ENTRYPOINT ["/bin/engula"]