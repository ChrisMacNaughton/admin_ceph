use std::process::Command;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;

use output_args::Args;


#[derive(Debug)]
pub enum LogType {
    TestMessage,
    CephMessage,
    CephDaemonOsdMessage,
    CephDaemonMonMessage,
    SmartMessage,
    // CephAdminResponse,
}

#[derive(Debug)]
pub struct LogMessage {
    pub log_type: LogType,
    pub json_body: String,
    pub osd_num: Option<u64>,
    pub drive_name: Option<String>,
    pub packet_header: Option<String>,
}

impl LogMessage {
    pub fn new(log_type: LogType, json_body: String) -> LogMessage {
        LogMessage {
            log_type: log_type,
            json_body: json_body,
            osd_num: None,
            drive_name: None,
            packet_header: None,
        }
    }
}

pub fn initialize_sender(args: Args) -> Sender<LogMessage> {
    let (tx, rx): (Sender<LogMessage>, Receiver<LogMessage>) = channel();
    thread::spawn(move || {
        debug!("Logging thread active");
        loop {
            match rx.recv() {
                Ok(msg) => handle_message(msg, &args),//debug!("{:?}", msg),
                Err(e) => {
                    error!("Had an error: {:?}", e);
                    continue
                }
            };
        }
    });
    tx
}

fn handle_message(message: LogMessage, args: &Args) {
    match message.log_type {
        LogType::TestMessage => {
           handle_string(&message, args);
        },
        LogType::CephMessage => {
            handle_ceph_message(&message, args);
        },
        LogType::CephDaemonMonMessage => {
            handle_daemon_mon_message(&message, args);
        },
        LogType::CephDaemonOsdMessage => {
            handle_daemon_osd_message(&message, args);
        },
        LogType::SmartMessage => {
            handle_smart_message(&message, args);
        },
        // _ => {}
    }
    // if message.string.is_some() {
    //     debug!("{:?}", message.string().unwrap());
    // } else if message.ceph_message.is_some() {
    //     log_ceph_message(message.ceph_message.unwrap());
    // } else if message.osd_message.is_some() {
    //     log_osd_message(message.osd_message.unwrap());
    // }
}

fn handle_string(message: &LogMessage, args: &Args) {
    trace!("String: {:?}", message);
}

fn handle_ceph_message(message: &LogMessage, args: &Args) {
    if args.influx.is_some() && args.outputs.contains(&"influx".to_string()) {
        json::handle_ceph_message(message, args);
    }
    if args.outputs.contains(&"stdout".to_string()) {
        info!("{:?}", message.json_body);
    }
}

fn hostname() -> String{
    let output = Command::new("hostname")
                         .output()
                         .unwrap_or_else(|e| panic!("failed to execute hostname: {}", e));
    let host = match String::from_utf8(output.stdout) {
        Ok(v) => v.replace("\n", ""),
        Err(_) => "{}".to_string(),
   };
   trace!("Got hostname: '{}'", host);

   host
}

pub fn handle_daemon_mon_message(message: &LogMessage, args: &Args) {
    if args.influx.is_some() && args.outputs.contains(&"influx".to_string()) {
        json::handle_daemon_mon_message(message, args);
    }
    if args.outputs.contains(&"stdout".to_string()) {
        info!("{:?}", message.json_body);
    }
}

pub fn handle_daemon_osd_message(message: &LogMessage, args: &Args) {
    if args.influx.is_some() && args.outputs.contains(&"influx".to_string()) {
        json::handle_daemon_osd_message(message, args);
    }
    if args.outputs.contains(&"stdout".to_string()) {
        info!("{:?}", message.json_body);
    }
}

pub fn handle_smart_message(message: &LogMessage, args: &Args) {
    if args.influx.is_some() && args.outputs.contains(&"influx".to_string()) {
        json::handle_smart_message(message, args);
    }
    if args.outputs.contains(&"stdout".to_string()) {
        info!("{:?}", message.json_body);
    }
}

mod json {
    use hyper::*;
    use hyper::header::ContentType;
    use output_args::Args;
    use super::LogMessage;

    pub fn handle_daemon_mon_message(message: &LogMessage, args: &Args) {
        let influx = &args.influx.clone().unwrap();
        let host_string = format!("http://{}:{}/record_ceph?measurement=monitor&hostname={}", influx.host, influx.port, super::hostname());

        send_to_url(&host_string[..], &message.json_body[..]);
    }

    pub fn handle_daemon_osd_message(message: &LogMessage, args: &Args) {
        let influx = &args.influx.clone().unwrap();
        let drive_name = message.drive_name.clone().unwrap_or("".to_string());
        let osd_num = message.osd_num.clone().unwrap_or(0);
        let host_string = format!("http://{}:{}/record_ceph?measurement=osd&osd_num={}&drive_name={}&hostname={}", influx.host, influx.port, osd_num, drive_name, super::hostname());
        send_to_url(&host_string[..], &message.json_body[..]);
    }

    pub fn handle_smart_message(message: &LogMessage, args: &Args) {
        let influx = &args.influx.clone().unwrap();
        let drive_name = message.drive_name.clone().unwrap_or("".to_string());
        let osd_num = message.osd_num.clone().unwrap_or(0);
        let host_string = format!("http://{}:{}/record_ceph?measurement=smart&osd_num={}&drive_name={}&hostname={}", influx.host, influx.port, osd_num, drive_name, super::hostname());
        send_to_url(&host_string[..], &message.json_body[..]);
    }

    pub fn handle_ceph_message(message: &LogMessage, args: &Args) {
        let influx = &args.influx.clone().unwrap();
        let host_string = format!("http://{}:{}/record_ceph?measurement=packet&hostname={}", influx.host, influx.port, super::hostname());
        let json_body = format!(r#"{{"ceph_packet": {}, "header": {}}}"#, &message.json_body, &message.packet_header.clone().unwrap());
        send_to_url(&host_string[..], &json_body[..]);
    }

    fn send_to_url(url: &str, body: &str) {
        trace!("Sending log message to {}", url);
        let client = Client::new();
        let _ = client.post(url)
            .body(body)
            .header(ContentType::json())
            .send();
    }
}
