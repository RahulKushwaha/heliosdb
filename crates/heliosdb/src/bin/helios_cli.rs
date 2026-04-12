//! Minimal CLI for manual testing: put, get, delete, scan.
//!
//! Usage:
//!   helios_cli <db_dir> put <key> <value>
//!   helios_cli <db_dir> get <key>
//!   helios_cli <db_dir> delete <key>
//!   helios_cli <db_dir> scan <start> <end>
//!   helios_cli <db_dir> flush

use heliosdb::{Options, DB};

fn main() -> heliosdb::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: helios_cli <db_dir> <command> [args...]");
        std::process::exit(1);
    }

    let db_dir = &args[1];
    let cmd    = &args[2];
    let db     = DB::open(db_dir, Options::default())?;

    match cmd.as_str() {
        "put" => {
            if args.len() < 5 { eprintln!("put <key> <value>"); std::process::exit(1); }
            db.put(args[3].as_bytes(), args[4].as_bytes())?;
            println!("OK");
        }
        "get" => {
            if args.len() < 4 { eprintln!("get <key>"); std::process::exit(1); }
            match db.get(args[3].as_bytes())? {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None    => println!("(not found)"),
            }
        }
        "delete" => {
            if args.len() < 4 { eprintln!("delete <key>"); std::process::exit(1); }
            db.delete(args[3].as_bytes())?;
            println!("OK");
        }
        "scan" => {
            if args.len() < 5 { eprintln!("scan <start> <end>"); std::process::exit(1); }
            let pairs = db.scan(args[3].as_bytes(), args[4].as_bytes())?;
            for (k, v) in pairs {
                println!("{} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
            }
        }
        "flush" => {
            db.flush()?;
            println!("flushed");
        }
        _ => {
            eprintln!("Unknown command: {cmd}");
            std::process::exit(1);
        }
    }

    Ok(())
}
