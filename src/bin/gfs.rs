use std::io::{Read, Write};
use std::process::ExitCode;

use gfs::client::{Client, Error};
use gfs::common::config::{Config, parse_size};

const USAGE: &str = "usage: gfs [--key=value ...] <command> [args...]
commands:
  create <path>
  rm <path>
  mv <source> <target>
  snapshot <source> <target>
  ls [-a] <directory>
  stat <path>
  read <path> [offset] [length]
  write <path> [offset]
  append <path>
";

fn fail(message: &str) -> ExitCode {
    eprintln!("{message}");
    ExitCode::from(1)
}

fn usage() -> ExitCode {
    eprint!("{USAGE}{}", Config::usage());
    ExitCode::from(2)
}

fn check(result: Result<(), Error>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => fail(&e.to_string()),
    }
}

fn read_stdin() -> Vec<u8> {
    let mut data = Vec::new();
    let _ = std::io::stdin().read_to_end(&mut data);
    data
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut config = Config::default();
    let positional = match config.apply_flags(std::env::args().skip(1)) {
        Ok(positional) => positional,
        Err(message) => {
            eprintln!("{message}");
            return usage();
        }
    };
    let Some((command, args)) = positional.split_first() else { return usage() };
    let client = Client::new(config);

    match (command.as_str(), args) {
        ("create", [path]) => check(client.create(path).await),
        ("rm", [path]) => check(client.remove(path).await),
        ("mv", [source, target]) => check(client.rename(source, target).await),
        ("snapshot", [source, target]) => check(client.snapshot(source, target).await),
        ("ls", [directory]) | ("ls", [_, directory]) if args.len() == 1 || args[0] == "-a" => {
            let entries = match client.list(directory, args.len() == 2).await {
                Ok(entries) => entries,
                Err(e) => return fail(&e.to_string()),
            };
            let mut out = std::io::stdout().lock();
            for entry in entries {
                let _ = writeln!(out, "{}{}", entry.name, if entry.is_directory { "/" } else { "" });
            }
            ExitCode::SUCCESS
        }
        ("stat", [path]) => {
            let info = match client.open(path).await {
                Ok(info) => info,
                Err(e) => return fail(&e.to_string()),
            };
            match client.length(path).await {
                Ok(length) => {
                    println!("chunks: {}\nlength: {length}", info.chunk_count);
                    ExitCode::SUCCESS
                }
                Err(e) => fail(&e.to_string()),
            }
        }
        ("read", [path, rest @ ..]) if rest.len() <= 2 => {
            let mut offset = 0;
            let mut length = 0;
            let bounded = rest.len() == 2;
            if let Some(text) = rest.first() {
                let Some(parsed) = parse_size(text) else { return fail("bad offset") };
                offset = parsed;
            }
            if bounded {
                let Some(parsed) = parse_size(&rest[1]) else { return fail("bad length") };
                length = parsed;
            }
            let step: u64 = 1 << 20;
            let mut remaining = if bounded { length } else { step };
            let mut out = std::io::stdout().lock();
            while remaining > 0 {
                let want = if bounded { remaining.min(step) } else { step };
                let data = match client.read(path, offset, want).await {
                    Ok(data) => data,
                    Err(e) => return fail(&e.to_string()),
                };
                let _ = out.write_all(&data);
                offset += data.len() as u64;
                if bounded {
                    remaining -= remaining.min(want);
                }
                if (data.len() as u64) < want {
                    break;
                }
            }
            let _ = out.flush();
            ExitCode::SUCCESS
        }
        ("write", [path, rest @ ..]) if rest.len() <= 1 => {
            let mut offset = 0;
            if let Some(text) = rest.first() {
                let Some(parsed) = parse_size(text) else { return fail("bad offset") };
                offset = parsed;
            }
            check(client.write(path, offset, &read_stdin()).await)
        }
        ("append", [path]) => match client.record_append(path, &read_stdin()).await {
            Ok(offset) => {
                println!("{offset}");
                ExitCode::SUCCESS
            }
            Err(e) => fail(&e.to_string()),
        },
        _ => usage(),
    }
}
