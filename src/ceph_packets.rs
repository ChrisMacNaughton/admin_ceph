use std::thread;

use ceph::sniffer::*;
use ceph::sniffer::serial::CephMessageWithHeader;
use pcap::{Capture, Device};

use Args;

use influent::measurement::{Measurement, Value};
use influent::create_client;
use influent::client::{Credentials, Precision, Client};

use time;

pub fn initialize_pcap(args: &Args) {
    let args = args.clone();
    thread::spawn(move || {
        // let _ = log_queue.send(LogMessage::new(LogType::TestMessage, "{\"msg\": \"New thread with send queue for Ceph packet sniffing\"}".to_string() ));
        debug!("Packet Sniffing thread active");
        let hostname: &str = &args.hostname[..];
        let device = match any_device() {
            Some(dev) => dev,
            None => return,
        };
        let device_name = device.name.clone();

        let mut cap = Capture::from_device(device).unwrap() //open the device
                              .promisc(false)
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
        let mut user = String::new();
        let mut password = String::new();
        let credentials: Credentials;
        let host: String;
        let mut hosts: Vec<&str> = vec![];

        let do_influx = args.influx.is_some() && args.outputs.contains(&"influx".to_string());
        let influx = args.influx;
        let client = if do_influx {
            let influx = influx.unwrap();
            user = influx.user.clone();
            password = influx.password.clone();
            credentials = Credentials {
                username: &user[..],
                password: &password[..],
                database: "ceph"
            };
            host = format!("http://{}:{}", influx.host, influx.port);
            hosts = vec![&host[..]];
            create_client(credentials, hosts)
        } else {
            credentials = Credentials {
                username: &user[..],
                password: &password[..],
                database: "",
            };
            create_client(credentials, hosts)
        };
        //Infinite loop


        loop {
            match cap.next(){
                //We received a packet
                Ok(packet) =>{
                    match serial::parse_ceph_packet(&packet.data) {
                        Some(result) => {
                            // let header = &result.header;
                            // let _ = log_queue.send(LogMessage::new(LogType::CephMessage, json::encode(&result).unwrap() ));
                            match result.ceph_message.message{
                                serial::Message::OsdOp(_) =>{
                                    trace!("logging: {:?}", result);

                                    // let _ = log_queue.send(LogMessage {
                                    //     log_type: LogType::CephMessage,
                                    //     json_body: None,
                                    //     // json_body: json::encode(&result.ceph_message.message).unwrap(),
                                    //     // packet_header: Some(json::encode(&header).unwrap()),
                                    //     osd_num: None,
                                    //     drive_name: None,
                                    //     ceph_msg: Some(result),
                                    // });
                                    if do_influx {
                                        log_to_influx(result, &client, hostname);
                                    }
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

fn log_to_influx(msg: CephMessageWithHeader, client: &Client, hostname: &str) {
    match msg.ceph_message.message {
        serial::Message::OsdOp(ref osd_op) => {
            let flags = format!("{:?}", osd_op.flags);
            let src_addr = msg.header.src_addr.ip_address();
            let dst_addr = msg.header.dst_addr.ip_address();
            let mut measurement = Measurement::new("osd_operation");
            measurement.set_timestamp(time::now().to_timespec().sec as i32);
            measurement.add_tag("type", "osd_op");
            measurement.add_tag("hostname", hostname);
            measurement.add_tag("src_ip", &src_addr[..]);
            measurement.add_tag("dst_ip", &dst_addr[..]);

            measurement.add_field("size", Value::Integer(osd_op.operation.payload_size as i64));
            measurement.add_field("operation", Value::String(&flags[..]));
            measurement.add_field("count", Value::Integer(osd_op.operation_count as i64));

            if osd_op.flags.contains(serial::CEPH_OSD_FLAG_WRITE) {
                measurement.add_tag("type", "write");
            } else if osd_op.flags.contains(serial::CEPH_OSD_FLAG_READ) {
                measurement.add_tag("type", "read");
            } else {
                trace!("{:?} doesn't contain {:?}", osd_op.flags, vec![serial::CEPH_OSD_FLAG_WRITE, serial::CEPH_OSD_FLAG_READ]);
            }
            let _ = client.write_one(measurement, Some(Precision::Seconds));
        },
        _ => {}
    }
    // debug!("About to log {:?}", measurements);
    // let _ = client.write_many(measurements, Some(Precision::Seconds));
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
