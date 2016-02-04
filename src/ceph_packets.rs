use std::sync::mpsc::Sender;
use std::thread;

use ceph::sniffer::*;
use pcap::{Capture, Device};
use rustc_serialize::json;

use messaging::LogMessage;
use messaging::LogType;

pub fn initialize_pcap(message_queue: &Sender<LogMessage>) {
    let log_queue: Sender<LogMessage> = message_queue.clone();
    thread::spawn(move || {
        // let _ = log_queue.send(LogMessage::new(LogType::TestMessage, "{\"msg\": \"New thread with send queue for Ceph packet sniffing\"}".to_string() ));
        debug!("Packet Sniffing thread active");
        let device = match any_device() {
            Some(dev) => dev,
            None => return,
        };
        let device_name = device.name.clone();

        let mut cap = Capture::from_device(device).unwrap() //open the device
                              .promisc(true)
                              //.snaplen(500) //Might need this still if we're losing packets
                              .timeout(100)
                              .open() //activate the handle
                              .unwrap(); //assume activation worked

        debug!("Setting up filter({})", &device_name);
        //Grab both monitor and OSD traffic
        match cap.filter("tcp dst portrange 6789-7300"){
            Ok(_) => {
                debug!("Filter successful({})", &device_name);
            },
            Err(e) => {
                error!("Invalid capture filter({}). Error: {:?}", &device_name, e);
                return;
            }
        }
        debug!("Waiting for packets({})", &device_name);
        //Grab some packets :)

        //Infinite loop
        loop {
            match cap.next(){
                //We received a packet
                Ok(packet) =>{
                    match serial::parse_ceph_packet(&packet.data) {
                        Some(result) => {
                            let header = &result.header;
                            // let _ = log_queue.send(LogMessage::new(LogType::CephMessage, json::encode(&result).unwrap() ));
                            match result.ceph_message.message{
                                serial::Message::OsdOp(_) =>{
                                    trace!("logging: {:?}", result);

                                    let _ = log_queue.send(LogMessage {
                                        log_type: LogType::CephMessage,
                                        json_body: json::encode(&result.ceph_message.message).unwrap(),
                                        packet_header: Some(json::encode(&header).unwrap()),
                                        osd_num: None,
                                        drive_name: None,
                                    });
                                },
                                //TODO: What should we do here?
                                //serial::Message::OsdSubop(ref sub_op) => sub_op,
                                _ => {}
                            };
                            // let _ = process_packet(&result.header, &result.ceph_message, &args);
                            // let _ =
                        },
                        _ => {},
                    };
                    // break
                },
                //We missed a packet, ignore
                Err(_) => {},
            }
        }
    });
}

fn any_device() -> Option<Device> {
    let dev_list = match Device::list(){
        Ok(l) => l,
        Err(e) => {
            error!("Unable to list network devices.  Error: {}", e);
            return None;
        }
    };

    for dev_device in dev_list {
        if dev_device.name == "any" {
            debug!("Found Network device {}", &dev_device.name);
            debug!("Setting up capture({})", &dev_device.name);
            return Some(dev_device);
        }
    }
    None
}
