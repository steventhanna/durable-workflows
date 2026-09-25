//! `durable-trace`: dumps the traces that `durable-workflows` tests record
//! with the `trace-model` feature and replays them through the Quint model
//! (`docs/design/trace-checking.md`).
//!
//! The backend is the `durable-workflows` feature (`mysql` or `postgres`).

mod dump;
mod gen;
mod report;

use std::{path::PathBuf, process::ExitCode};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "durable-trace",
    about = "Trace checking against the Quint model"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Writes one trace JSON per `dwt_%` test database that has a `durable_trace` table.
    Dump {
        /// Server URL (the database part is replaced per test database).
        #[arg(long)]
        server: String,
        #[arg(long)]
        out: PathBuf,
        /// Dump only this database.
        #[arg(long)]
        db: Option<String>,
        /// Dump only traces whose test name contains this text.
        #[arg(long)]
        test: Option<String>,
    },
    /// Generates one Quint module per trace plus `index.json`.
    Gen {
        /// A trace JSON file or a directory of them.
        #[arg(long = "in")]
        input: PathBuf,
        #[arg(long)]
        out: PathBuf,
        /// YAML or JSON map of test-name globs to the invariant each gap test
        /// violates (`spec/traces/gaps.yaml`).
        #[arg(long = "expect-violation")]
        expect_violation: Option<PathBuf>,
    },
    /// Explains a failing step: names it from a `quint test --match '^step_'`
    /// log, and with `--qnt` diffs the model's values against the recorded ones.
    Report {
        #[arg(long)]
        trace: PathBuf,
        #[arg(long = "quint-output")]
        quint_output: Option<PathBuf>,
        /// The generated module; replays the step in the Quint REPL for a field diff.
        #[arg(long)]
        qnt: Option<PathBuf>,
        /// The failing step (instead of reading it from `--quint-output`).
        #[arg(long)]
        step: Option<usize>,
        /// The Quint executable.
        #[arg(long, default_value = "quint")]
        quint: PathBuf,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Dump {
            server,
            out,
            db,
            test,
        } => dump::run(&server, &out, db.as_deref(), test.as_deref()).await,
        Command::Gen {
            input,
            out,
            expect_violation,
        } => gen::run(&input, &out, expect_violation.as_deref()),
        Command::Report {
            trace,
            quint_output,
            qnt,
            step,
            quint,
        } => report::run(&report::Request {
            trace: &trace,
            quint_output: quint_output.as_deref(),
            qnt: qnt.as_deref(),
            step,
            quint: &quint,
        }),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("durable-trace: {error}");
            ExitCode::FAILURE
        }
    }
}

/// File-name-safe form of a test name (`a::b` -> `a__b`).
pub(crate) fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
