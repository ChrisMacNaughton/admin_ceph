use std::fs;
use std::io;
use std::path::Path;
use std::str::FromStr;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;

use ceph::{osd_mount_point, get_osd_perf_dump_raw};
use regex::Regex;

use messaging::LogMessage;
use messaging::LogType;

//NOTE: This skips a lot of failure cases
// Check for osd sockets and give back a vec of osd numbers that are active
fn get_osds_with_match() -> Result<Vec<u64>, io::Error> {
    let mut osds: Vec<u64> = Vec::new();

    let osd_regex = Regex::new(r"ceph-osd.(?P<number>\d+).asok").unwrap();

    for entry in try!(fs::read_dir(Path::new("/var/run/ceph"))){
        //parse the unix socket names such as:
        //ceph-mon.ip-172-31-22-89.asok
        //ceph-osd.1.asok

        let entry = try!(entry);
        let sock_addr_osstr = entry.file_name();
        let file_name = match sock_addr_osstr.to_str(){
            Some(name) => name,
            None => {
                //Skip files we can't turn into a string
                continue;
            }
        };

        //Ignore failures
        match osd_regex.captures(file_name){
            Some(osd) => {
                if let Some(osd_number) = osd.name("number"){
                    let num = u64::from_str(osd_number).unwrap();
                    osds.push(num);
                }
                //Ignore failures
            }
            //Ignore non matches, ie: ceph monitors
            None => {},
        }
    }
    return Ok(osds);
}

fn get_osds() -> Vec<u64> {
    match get_osds_with_match() {
        Ok(list) => list,
        Err(_) => {
            info!("No OSDs found");
            vec![]
        }
    }
}

pub fn initialize_osd_scanner(message_queue: &Sender<LogMessage>) {
    let log_queue: Sender<LogMessage> = message_queue.clone();
    thread::spawn(move || {
        // let _ = log_queue.send(LogMessage::new(LogType::TestMessage, "{\"msg\": \"New thread with send queue for Ceph Monitor checking\"}".to_string() ));
        debug!("OSD thread active");
        let periodic = timer_periodic(5000);

        let mut osd_list = get_osds();
        debug!("OSDs on this host: {:?}", osd_list);
        let mut i = 0;
        loop {
            trace!("Going around OSD loop again!");
            i = i + 1;
            for osd_num in osd_list.clone().iter(){
                // match ceph::get_osd_perf_dump(osd_num) {
                match get_osd_perf_dump_raw(osd_num) {
                    Some(osd) => {
                        let drive_name = osd_mount_point(osd_num).unwrap_or("".to_string());
                        // logging::osd_perf::log(osd, &args, *osd_num, &drive_name);
                        // logging::json::log_osd(osd, &args, *osd_num, &drive_name);
                        let _ = log_queue.send(LogMessage{ log_type: LogType::CephDaemonOsdMessage, json_body: osd, osd_num: Some(*osd_num), drive_name: Some(drive_name), packet_header: None});
                    },
                    None => continue,
                }
            }
            osd_list = match i % 10 {
                0 => get_osds(),
                _ => osd_list,
            };
            let _ = periodic.recv();
        }
    });
}

fn timer_periodic(ms: u32) -> Receiver<()> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        loop {
            thread::sleep_ms(ms);
            if tx.send(()).is_err() {
                break;
            }
        }
    });
    rx
}
