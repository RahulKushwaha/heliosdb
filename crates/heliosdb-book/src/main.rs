//! helios-book — generates the heliosDB mdBook source and optionally builds/serves it.
//!
//! Usage:
//!   helios-book generate [--out <dir>]   # write book source to <dir> (default: ./book-src)
//!   helios-book build    [--out <dir>]   # generate then run `mdbook build`
//!   helios-book serve    [--out <dir>]   # generate then run `mdbook serve`

mod chapters;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "helios-book", about = "Generate the heliosDB mdBook documentation")]
struct Cli {
    #[command(subcommand)]
    command: Option<Cmd>,

    /// Output directory for the generated book source (default: ./book-src)
    #[arg(long, default_value = "book-src")]
    out: PathBuf,
}

#[derive(Subcommand)]
enum Cmd {
    /// Write all book source files to --out (default)
    Generate,
    /// Generate then run `mdbook build`
    Build,
    /// Generate then run `mdbook serve --open`
    Serve,
}

fn main() {
    let cli = Cli::parse();
    let out = &cli.out;

    generate(out).expect("failed to generate book source");

    match cli.command.unwrap_or(Cmd::Generate) {
        Cmd::Generate => {
            println!("Book source written to {}", out.display());
            println!("Run `mdbook build {}` or `helios-book build` to compile.", out.display());
        }
        Cmd::Build => {
            println!("Book source written to {}", out.display());
            run_mdbook(&["build", out.to_str().unwrap()]);
        }
        Cmd::Serve => {
            println!("Book source written to {}", out.display());
            run_mdbook(&["serve", out.to_str().unwrap(), "--open"]);
        }
    }
}

// ---------------------------------------------------------------------------
// Top-level generator
// ---------------------------------------------------------------------------

fn generate(out: &Path) -> std::io::Result<()> {
    // Directory layout
    let dirs = [
        out.to_path_buf(),
        out.join("architecture"),
        out.join("internals"),
        out.join("api"),
        out.join("benchmarks"),
    ];
    for d in &dirs {
        fs::create_dir_all(d)?;
    }

    // book.toml
    write(out, "book.toml", chapters::book_toml())?;

    // SUMMARY.md
    write(out, "SUMMARY.md", chapters::summary())?;

    // Introduction
    write(out, "introduction.md", chapters::introduction())?;

    // Architecture
    write(out.join("architecture"), "overview.md",        chapters::arch_overview())?;
    write(out.join("architecture"), "active-inactive.md", chapters::arch_active_inactive())?;
    write(out.join("architecture"), "file-format.md",     chapters::arch_file_format())?;
    write(out.join("architecture"), "write-read-paths.md",chapters::arch_write_read_paths())?;

    // Internals
    write(out.join("internals"), "block.md",      chapters::internals_block())?;
    write(out.join("internals"), "bloom.md",      chapters::internals_bloom())?;
    write(out.join("internals"), "wal.md",        chapters::internals_wal())?;
    write(out.join("internals"), "memtable.md",   chapters::internals_memtable())?;
    write(out.join("internals"), "compaction.md", chapters::internals_compaction())?;

    // API
    write(out.join("api"), "getting-started.md", chapters::api_getting_started())?;
    write(out.join("api"), "reference.md",       chapters::api_reference())?;

    // Benchmarks
    write(out.join("benchmarks"), "methodology.md", chapters::bench_methodology())?;
    write(out.join("benchmarks"), "running.md",     chapters::bench_running())?;

    Ok(())
}

fn write(dir: impl AsRef<Path>, name: &str, content: &str) -> std::io::Result<()> {
    let path = dir.as_ref().join(name);
    fs::write(&path, content)?;
    println!("  wrote {}", path.display());
    Ok(())
}

fn run_mdbook(args: &[&str]) {
    let status = Command::new("mdbook")
        .args(args)
        .status()
        .unwrap_or_else(|_| {
            eprintln!("error: `mdbook` not found. Install with: cargo install mdbook");
            std::process::exit(1);
        });
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}
