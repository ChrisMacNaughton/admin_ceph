extern crate ceph;
#[macro_use] extern crate clap;
extern crate hyper;
#[macro_use] extern crate log;
extern crate pcap;
extern crate regex;
extern crate rustc_serialize;
extern crate simple_logger;
extern crate yaml_rust;

use std::fs::File;
use std::io::prelude::*;

use clap::{Arg, App};
use log::LogLevel;
use yaml_rust::YamlLoader;

mod messaging;
mod ceph_monitor;
mod ceph_osd;
mod ceph_packets;

#[cfg(test)]
mod tests{
    use log::LogLevel;
    #[test]
    fn test_parse_file() {
        let file = r#"
outputs:
  - stdout
  - influx
influx:
  host: 127.0.0.1
  port: 8086
  user: root
  password: root
"#;
        let args = super::parse(file, LogLevel::Info).unwrap();

        assert_eq!(args.outputs, vec!["stdout", "influx"]);
        assert_eq!(args.influx.unwrap().port, "8086");
    }
}

#[derive(Clone,Debug)]
pub struct Args {
    pub influx: Option<Influx>,
    pub stdout: Option<String>,
    pub outputs: Vec<String>,
    pub config_path: String,
    pub log_level: log::LogLevel,
}

struct CliArgs {
    log_level: log::LogLevel,
    config_file: String,
}

impl Args {
    fn clean() -> Args {
        Args {
            influx: None,
            stdout: None,
            outputs: Vec::new(),
            config_path: "".to_string(),
            log_level: LogLevel::Info,
        }
    }
    fn with_log_level(log_level: LogLevel) -> Args {
        Args {
            influx: None,
            stdout: None,
            outputs: Vec::new(),
            config_path: "".to_string(),
            log_level: log_level,
        }
    }
}

#[derive(Clone,Debug)]
pub struct Influx {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String
}

fn main() {
    println!("Starting program");
    let args = get_args();
    simple_logger::init_with_level(args.log_level).unwrap();
    info!("Logging with: {:?}", args);
    let output_sender = messaging::initialize_sender(args.clone());
    ceph_monitor::initialize_monitor_scanner(&output_sender);
    ceph_packets::initialize_pcap(&output_sender);
    ceph_osd::initialize_osd_scanner(&output_sender);
    loop {
        std::thread::sleep(std::time::Duration::new(10, 0));
    }
}

fn get_args() -> Args {
    let cli_args = get_cli_args();
    let yaml_text = match read_from_file(cli_args.config_file.as_ref()) {
        Ok(yaml) => yaml,
        Err(_) => "".to_string(),
    };
    let args = parse(yaml_text.as_ref(), cli_args.log_level).unwrap_or(Args::clean());
    args
}

fn get_cli_args() -> CliArgs {
    let matches = App::new("admin_ceph")
        .version(crate_version!())
        .arg(Arg::with_name("debug")
                           .short("d")
                           .multiple(true)
                           .help("Sets the level of debugging information"))
        .arg(Arg::with_name("CONFIG")
                           .short("c")
                           .long("config")
                           .help("Sets a custom config file")
                           .takes_value(true))
        .get_matches();
    // let matches = clap_app!(args =>
    //     (version: &version[..])
    //     (@arg CONFIG: -c --config +takes_value "Path to config file")
    //     (@arg debug: -d ... "Sets the level of debugging information")
    // ).get_matches();

    let log_level = match matches.occurrences_of("debug") {
        0 => log::LogLevel::Warn,
        1 => log::LogLevel::Info,
        2 => log::LogLevel::Debug,
        3 | _ => log::LogLevel::Trace,
    };
    CliArgs {
        log_level: log_level,
        config_file: matches.value_of("CONFIG").unwrap_or("/etc/default/decode_ceph.yaml").to_string()
    }
}

fn parse(args_string: &str, log_level: LogLevel) -> Result<Args, String> {
    let config_path = "/etc/default/decode_ceph.yaml";

    // Remove this hack when the new version of yaml_rust releases to get the real
    // error msg
    let docs = match YamlLoader::load_from_str(&args_string) {
        Ok(data) => data,
        Err(_) => {
            // error!("Unable to load yaml data from config file");
            return Err("cannot load data from yaml".to_string());
        }
    };
    if docs.len() == 0 {
        return Ok(Args::with_log_level(log_level));
    }
    let doc = &docs[0];

    let stdout = match doc["stdout"].as_str() {
        Some(o) => Some(o.to_string()),
        None => None
    };
    let influx_doc = doc["influx"].clone();
    let influx_host = influx_doc["host"].as_str().unwrap_or("127.0.0.1");
    let influx_port = influx_doc["port"].as_str().unwrap_or("8086");
    let influx_password = influx_doc["password"].as_str().unwrap_or("root");
    let influx_user = influx_doc["user"].as_str().unwrap_or("root");
    let influx = Influx {
        host: influx_host.to_string(),
        port: influx_port.to_string(),
        password: influx_password.to_string(),
        user: influx_user.to_string(),
    };


    let outputs: Vec<String> = match doc["outputs"].as_vec() {
        Some(o) => {
            o.iter()
             .map(|x| {
                 match x.as_str() {
                     Some(o) => o.to_string(),
                     None => "".to_string(),
                 }
             })
             .collect()
        }
        None => Vec::new(),
    };

    Ok(Args {
        stdout: stdout,
        influx: Some(influx),
        outputs: outputs,
        log_level: log_level,
        config_path: config_path.to_string(),
    })
}

fn read_from_file(config_path: &str) -> Result<String, String> {
    let mut f = try!(File::open(config_path).map_err(|e| e.to_string()));

    let mut s = String::new();
    try!(f.read_to_string(&mut s).map_err(|e| e.to_string()));
    Ok(s.to_string())
}
