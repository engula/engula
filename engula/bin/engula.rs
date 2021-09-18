mod bench;
mod config;
mod start;

use std::net::SocketAddr;

use clap::Clap;
use metrics::{register_counter, register_histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Clap)]
struct Command {
    #[clap(long, default_value = "/tmp/engula")]
    log_dir: String,
    #[clap(long, default_value = "127.0.0.1:14268")]
    jaeger_addr: String,
    #[clap(long, default_value = "127.0.0.1:19090")]
    prometheus_addr: String,
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    Bench(bench::Command),
    Start(start::Command),
}

#[tokio::main]
async fn main() {
    let cmd: Command = Command::parse();

    let filter = tracing_subscriber::filter::LevelFilter::INFO;

    let appender = tracing_appender::rolling::never(&cmd.log_dir, "LOG");
    let (writer, _writer_guard) = tracing_appender::non_blocking(appender);
    let fmt_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_writer(writer)
        .with_filter(filter);

    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("engula")
        .with_collector_endpoint(format!("http://{}/api/traces", cmd.jaeger_addr))
        .install_batch(opentelemetry::runtime::Tokio)
        .unwrap();
    let otel_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(filter);
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(otel_layer)
        .init();
    init_prometheus(&cmd.prometheus_addr);

    match &cmd.subcmd {
        SubCommand::Bench(cmd) => cmd.run().await.unwrap(),
        SubCommand::Start(cmd) => cmd.run().await.unwrap(),
    }

    opentelemetry::global::shutdown_tracer_provider();
}

fn init_prometheus(addr: &str) {
    let addr: SocketAddr = addr.parse().unwrap();
    PrometheusBuilder::new()
        .listen_address(addr)
        .install()
        .unwrap();

    register_histogram!("engula.get.us");
    register_histogram!("engula.put.us");
    register_counter!("engula.cache.hit");
    register_counter!("engula.cache.miss");

    register_counter!("engula.flush.bytes");
    register_histogram!("engula.flush.throughput");
    register_counter!("engula.compact.bytes");
    register_histogram!("engula.compact.throughput");

    register_counter!("engula.fs.s3.read.bytes");
    register_histogram!("engula.fs.s3.read.throughput");
    register_counter!("engula.fs.s3.writer.bytes");
    register_histogram!("engula.fs.s3.write.throughput");
    register_histogram!("engula.fs.s3.sync.seconds");

    register_counter!("engula.fs.local.read.bytes");
    register_histogram!("engula.fs.local.read.throughput");
    register_counter!("engula.fs.local.writer.bytes");
    register_histogram!("engula.fs.local.write.throughput");
    register_histogram!("engula.fs.local.sync.seconds");

    register_counter!("engula.fs.remote.read.bytes");
    register_histogram!("engula.fs.remote.read.throughput");
    register_counter!("engula.fs.remote.writer.bytes");
    register_histogram!("engula.fs.remote.write.throughput");
    register_histogram!("engula.fs.remote.sync.seconds");
}
