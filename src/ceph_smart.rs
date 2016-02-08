use ceph::osd::mount_point;
use ceph_osd::get_osds;
use libatasmart::Disk;
use messaging::LogMessage;
use messaging::LogType;
use std::path::Path;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;
use std::time::Duration;

pub fn initialize_monitor_smart(message_queue: &Sender<LogMessage>) {
    let log_queue: Sender<LogMessage> = message_queue.clone();
    thread::spawn(move || {
            debug!("Monitor smart active");
            let periodic = timer_periodic(5000);

            let mut osd_list = get_osds();
            debug!("OSDs on this host: {:?}", osd_list);
            let mut i = 0;
            loop {
                trace!("Going around OSD loop again!");
                i = i + 1;
                for osd_num in osd_list.clone().iter(){
                    match mount_point(osd_num){
                        Some(drive_name) => {
                            let mut smart_ata = Disk::new(Path::new(&drive_name)).unwrap();
                            let smart_data = smart_ata.get_smart_status();
                            match smart_data{
                                Ok(d) => {
                                    let json_body = format!(r#"{{{}}}"#, d);
                                    let _ = log_queue.send(LogMessage{
                                        log_type: LogType::SmartMessage,
                                        json_body: json_body,
                                        osd_num: Some(*osd_num),
                                        drive_name: Some(drive_name),
                                        packet_header: None
                                    });
                                },
                                //TODO: Should we log smart ata errors?
                                Err(_) => continue,
                            }
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
            thread::sleep(Duration::from_millis(ms as u64));
            if tx.send(()).is_err() {
                break;
            }
        }
    });
    rx
}
