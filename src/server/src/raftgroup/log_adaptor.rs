// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// A [slog Drain](slog::Drain) that converts [records](slog::Record) into [tracing events](Event).
///
/// To use, create a [slog logger](slog::Logger) using an instance of [TracingSlogDrain] as its
/// drain:
///
/// ```rust
/// # use slog::*;
/// # use tracing_slog::TracingSlogDrain;
/// let drain = TracingSlogDrain;
/// let root = Logger::root(drain, o!());
///
/// info!(root, "logged using slogger");
/// ```
#[derive(Debug)]
pub struct TracingSlogDrain;

impl slog::Drain for TracingSlogDrain {
    type Ok = ();
    type Err = slog::Never;

    /// Converts a [slog record](slog::Record) into a [tracing event](Event)
    /// and dispatches it to any registered tracing subscribers
    /// using the [default dispatcher](dispatcher::get_default).
    /// Currently, the key-value pairs are ignored.
    fn log(
        &self,
        record: &slog::Record<'_>,
        _values: &slog::OwnedKVList,
    ) -> Result<Self::Ok, Self::Err> {
        let callsite = tracing::Callsite;
        let field_set = tracing::field::FieldSet::new(names, callsite);
        let metadata = tracing::Metadata::new(
            "slog event",
            "slog",
            to_tracing_level(record.level()),
            Some(record.file()),
            Some(record.line()),
            Some(record.module()),
            fields,
            tracing::metadata::Kind::EVENT,
        );
        tracing::dispatcher::get_default(|dispatch| {
            if !dispatch.enabled(&metadata) {
                return;
            }

            let values = tracing::ValueSet::new();
            let event = tracing::Event::new(&metadata, values);
            dispatch.event(&event);
        });
        Ok(())
    }
}

fn to_tracing_level(level: slog::Level) -> tracing::Level {
    match level {
        slog::Level::Critical => tracing::Level::ERROR,
        slog::Level::Error => tracing::Level::ERROR,
        slog::Level::Warning => tracing::Level::WARN,
        slog::Level::Info => tracing::Level::INFO,
        slog::Level::Debug => tracing::Level::DEBUG,
        slog::Level::Trace => tracing::Level::TRACE,
    }
}
