use Args;
use ceph::osd::mount_point;
use ceph_osd::get_osds;
use influent::measurement::{Measurement, Value};
use influent::create_client;
use influent::client::{Credentials, Precision, Client};
use libatasmart::Disk;

use std::path::Path;
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::Duration;
use time;

pub fn initialize_monitor_smart(args: &Args) {
    let args = args.clone();
    thread::spawn(move || {
        let hostname: &str = &args.hostname[..];
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

        debug!("Monitor smart active");
        //Run this every 30 minutes
        let periodic = timer_periodic(1800);

        let mut osd_list = get_osds();
        debug!("OSDs on this host: {:?}", osd_list);
        let mut i = 0;
        loop {
            trace!("Checking OSD smartata data again!");
            i = i + 1;
            for osd_num in osd_list.clone().iter(){
                match mount_point(osd_num){
                    Some(drive_name) => {
                        if do_influx {
                            let osd_num = format!("{}", osd_num);
                            log_to_influx(&client, hostname, &drive_name[..], &osd_num[..]);
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

fn log_to_influx(client: &Client, hostname: &str, drive_name: &str, osd_num: &str) {
    //Query the drive for smart status
    let mut smart_ata = match Disk::new(Path::new(&drive_name)){
        Ok(s) => s,
        Err(e) => {
            //This could mean that the drive is dead
            let mut measurement = Measurement::new("smart");
            measurement.set_timestamp(time::now().to_timespec().sec as i32);
            measurement.add_tag("hostname", hostname);
            measurement.add_tag("disk", drive_name);
            measurement.add_tag("osd_num", osd_num);
            measurement.add_field("error", Value::String(&e));

            let _ = client.write_one(measurement, Some(Precision::Seconds));
            return;
        },
    };
    let smart_data = smart_ata.get_smart_status();
    let temperature = smart_ata.get_temperature();
    let power_on_time = smart_ata.get_power_on();
    let overall_status = smart_ata.smart_get_overall();
    let bad_sector_count = smart_ata.get_bad_sectors();

    match smart_data{
        Ok(smart_ok) => {
            let mut measurement = Measurement::new("smart");
            measurement.set_timestamp(time::now().to_timespec().sec as i32);
            measurement.add_tag("hostname", hostname);
            measurement.add_tag("disk", drive_name);
            measurement.add_tag("osd_num", osd_num);
            measurement.add_field("smart_status", Value::Boolean(smart_ok));
            measurement.add_field("temperature_mkelvin", Value::Integer(
                temperature.unwrap_or(0 as u64) as i64));
            measurement.add_field("bad_sector_count", Value::Integer(
                bad_sector_count.unwrap_or(0 as u64) as i64));
            measurement.add_field("power_on_time", Value::Integer(
                power_on_time.unwrap_or(0 as u64) as i64));

            let _ = client.write_one(measurement, Some(Precision::Seconds));
        },
        Err(_) => {
            let mut measurement = Measurement::new("smart");
            measurement.set_timestamp(time::now().to_timespec().sec as i32);
            measurement.add_tag("hostname", hostname);
            measurement.add_tag("disk", drive_name);
            measurement.add_tag("osd_num", osd_num);
            measurement.add_field("smart_data_collection_error", Value::String(&e));

            let _ = client.write_one(measurement, Some(Precision::Seconds));
        },
    }
}

fn timer_periodic(seconds: u32) -> Receiver<()> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(seconds as u64));
            if tx.send(()).is_err() {
                break;
            }
        }
    });
    rx
}
