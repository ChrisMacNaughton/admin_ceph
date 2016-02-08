use std::fs;
use std::io;
use std::path::Path;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::time::Duration;

use ceph::*;

use messaging::LogMessage;
use messaging::LogType;

pub fn initialize_monitor_scanner(message_queue: &Sender<LogMessage>) {
    let log_queue: Sender<LogMessage> = message_queue.clone();
    thread::spawn(move || {
        // let _ = log_queue.send(LogMessage::new(LogType::TestMessage, "{\"msg\": \"New thread with send queue for Ceph Monitor checking\"}".to_string() ));
        debug!("Monitor thread active");
        let periodic = timer_periodic(5000);

        let mut is_monitor = check_is_monitor();

        let mut i = 0;
        loop {
            trace!("Going around Monitor loop again!");
            i = i + 1;
            if is_monitor {
                trace!("Getting MON info");
                let _: Option<String> = match get_monitor_perf_dump_raw() {
                    Some(dump) => {
                        let _ = log_queue.send(LogMessage::new(LogType::CephDaemonMonMessage, dump));
                        // logging::json::log(dump, &args);
                        None
                    },
                    None => {
                        is_monitor = check_is_monitor();
                        None
                    }
                };
            }
            is_monitor = match i % 10 {
                0 => check_is_monitor(),
                _ => is_monitor,
            };
            let _ = periodic.recv();
        }
    });
}

fn has_child_directory(dir: &Path) -> Result<bool, io::Error> {
    if try!(fs::metadata(dir)).is_dir() {
        for entry in try!(fs::read_dir(dir)) {
            let entry = try!(entry);
            if try!(fs::metadata(entry.path())).is_dir() {
                return Ok(true);
            }
        }
    }
    return Ok(false);
}

// Look for /var/lib/ceph/mon/ceph-ip-172-31-24-128
fn check_is_monitor() -> bool {
    // does it have a mon directory entry?
    match has_child_directory(Path::new("/var/lib/ceph/mon")){
        Ok(result) => result,
        Err(_) => {
            info!("No Monitor found");
            false
        }
    }
}

fn timer_periodic(ms: u32) -> Receiver<()> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(ms as u64));
            if tx.send(()).is_err() {
                break;
            }
        }
    });
    rx
}
