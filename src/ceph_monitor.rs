use std::fs;
use std::io;
use std::path::Path;
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::Duration;

use Args;
use ceph::*;

use influent::create_client;
use influent::client::Credentials;

pub fn initialize_monitor_scanner(args: &Args) {
    let args = args.clone();
    thread::spawn(move || {
        // let _ = log_queue.send(LogMessage::new(LogType::TestMessage, "{\"msg\": \"New thread with send queue for Ceph Monitor checking\"}".to_string() ));
        debug!("Monitor thread active");
        let periodic = timer_periodic(5000);

        let mut is_monitor = check_is_monitor();

        let mut i = 0;
        let do_influx = args.influx.is_some() && args.outputs.contains(&"influx".to_string());
        let mut user = String::new();
        let mut password = String::new();
        let credentials: Credentials;
        let host: String;
        let mut hosts: Vec<&str> = vec![];

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

        loop {
            trace!("Going around Monitor loop again!");
            i = i + 1;
            if is_monitor {
                trace!("Getting MON info");
                match get_monitor_perf_dump_raw() {
                    Some(dump) => {
                        // let _ = log_queue.send(LogMessage::new(LogType::CephDaemonMonMessage, dump));
                        if do_influx {
                            influx::log_to_influx(&dump, &client, &args.hostname);
                        }
                    },
                    None => {
                        is_monitor = check_is_monitor();
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

mod influx {
    use time;
    use influent::measurement::{Measurement, Value};
    use serde_json;
    use influent::client::{Precision, Client};
    #[cfg(test)]
    mod tests {
        #[test]
        fn test_parse_monitor_json() {
            let json = r#"{
    "cluster": {
        "num_mon": 3,
        "num_mon_quorum": 3,
        "num_osd": 3,
        "num_osd_up": 3,
        "num_osd_in": 3,
        "osd_epoch": 7,
        "osd_kb": 9386436,
        "osd_kb_used": 101760,
        "osd_kb_avail": 9284676,
        "num_pool": 3,
        "num_pg": 192,
        "num_pg_active_clean": 0,
        "num_pg_active": 0,
        "num_pg_peering": 0,
        "num_object": 0,
        "num_object_degraded": 0,
        "num_object_unfound": 0,
        "num_bytes": 0,
        "num_mds_up": 0,
        "num_mds_in": 0,
        "num_mds_failed": 0,
        "mds_epoch": 1
    },
    "leveldb": {
        "leveldb_get": 1549,
        "leveldb_transaction": 106,
        "leveldb_compact": 0,
        "leveldb_compact_range": 0,
        "leveldb_compact_queue_merge": 0,
        "leveldb_compact_queue_len": 0
    },
    "mon": {},
    "paxos": {
        "start_leader": 0,
        "start_peon": 2,
        "restart": 21,
        "refresh": 45,
        "refresh_latency": {
            "avgcount": 45,
            "sum": 0.153847
        },
        "begin": 45,
        "begin_keys": {
            "avgcount": 0,
            "sum": 0
        },
        "begin_bytes": {
            "avgcount": 45,
            "sum": 270667
        },
        "begin_latency": {
            "avgcount": 45,
            "sum": 0.407902
        },
        "commit": 45,
        "commit_keys": {
            "avgcount": 0,
            "sum": 0
        },
        "commit_bytes": {
            "avgcount": 0,
            "sum": 0
        },
        "commit_latency": {
            "avgcount": 0,
            "sum": 0
        },
        "collect": 2,
        "collect_keys": {
            "avgcount": 2,
            "sum": 2
        },
        "collect_bytes": {
            "avgcount": 2,
            "sum": 48
        },
        "collect_latency": {
            "avgcount": 2,
            "sum": 0.010538
        },
        "collect_uncommitted": 0,
        "collect_timeout": 0,
        "accept_timeout": 0,
        "lease_ack_timeout": 0,
        "lease_timeout": 0,
        "store_state": 45,
        "store_state_keys": {
            "avgcount": 45,
            "sum": 697
        },
        "store_state_bytes": {
            "avgcount": 45,
            "sum": 523062
        },
        "store_state_latency": {
            "avgcount": 45,
            "sum": 0.33984
        },
        "share_state": 0,
        "share_state_keys": {
            "avgcount": 0,
            "sum": 0
        },
        "share_state_bytes": {
            "avgcount": 0,
            "sum": 0
        },
        "new_pn": 0,
        "new_pn_latency": {
            "avgcount": 0,
            "sum": 0
        }
    },
    "throttle-mon_client_bytes": {
        "val": 55,
        "max": 104857600,
        "get": 82,
        "get_sum": 6712,
        "get_or_fail_fail": 0,
        "get_or_fail_success": 0,
        "take": 0,
        "take_sum": 0,
        "put": 81,
        "put_sum": 6657,
        "wait": {
            "avgcount": 0,
            "sum": 0
        }
    },
    "throttle-mon_daemon_bytes": {
        "val": 0,
        "max": 419430400,
        "get": 55,
        "get_sum": 89170,
        "get_or_fail_fail": 0,
        "get_or_fail_success": 0,
        "take": 0,
        "take_sum": 0,
        "put": 55,
        "put_sum": 89170,
        "wait": {
            "avgcount": 0,
            "sum": 0
        }
    },
    "throttle-msgr_dispatch_throttler-mon": {
        "val": 0,
        "max": 104857600,
        "get": 1381,
        "get_sum": 822091,
        "get_or_fail_fail": 0,
        "get_or_fail_success": 0,
        "take": 0,
        "take_sum": 0,
        "put": 1381,
        "put_sum": 822091,
        "wait": {
            "avgcount": 0,
            "sum": 0
        }
    }
}"#.to_string();
            let measurement = super::MonMeasurement::from_json(&json).unwrap();

            assert_eq!(measurement.used, 101760);
            assert_eq!(measurement.osds_up, 3);
            assert_eq!(measurement.monitors_quorum, 3);
        }
    }

    macro_rules! find_u64 {
        ($container:ident, $name:expr) => {
            match $container.lookup($name) {
                Some(s) => {
                    s.as_u64().unwrap_or(0)
                }, None => 0
            }
        }
    }

    struct MonMeasurement {
        used: u64,
        avail: u64,
        total: u64,
        osds: u64,
        osds_up: u64,
        osds_in: u64,
        osd_epoch: u64,
        pgs: u64,
        pgs_active_clean: u64,
        ppgs_active: u64,
        pgs_peering: u64,
        objects: u64,
        objects_degraded: u64,
        objects_unfound: u64,
        monitors: u64,
        monitors_quorum: u64,
    }

    impl MonMeasurement {
        pub fn from_json(json_string: &String) -> Option<MonMeasurement> {
            let value: Option<serde_json::Value> = match serde_json::from_str(&json_string[..]){
                Ok(s) => Some(s),
                Err(e) => {
                    debug!("Problem parsing json: {:?}", e);
                    None
                }
            };
            match value {
                Some(s) => {
                    Some(MonMeasurement {
                        used: find_u64!(s, "cluster.osd_kb_used"),
                        avail: find_u64!(s, "cluster.osd_kb_avail"),
                        total: find_u64!(s, "cluster.osd_kb"),
                        osds: find_u64!(s, "cluster.num_osd"),
                        osds_up: find_u64!(s, "cluster.num_osd_up"),
                        osds_in: find_u64!(s, "cluster.num_osd_in"),
                        osd_epoch: find_u64!(s, "cluster.osd_epoch"),
                        pgs: find_u64!(s, "cluster.num_pg"),
                        pgs_active_clean: find_u64!(s, "cluster.num_pg_active_clean"),
                        ppgs_active: find_u64!(s, "cluster.num_pg_active"),
                        pgs_peering: find_u64!(s, "cluster.num_pg_peering"),
                        objects: find_u64!(s, "cluster.num_object"),
                        objects_degraded: find_u64!(s, "cluster.num_object_degraded"),
                        objects_unfound: find_u64!(s, "cluster.num_object_unfound"),
                        monitors: find_u64!(s, "cluster.num_mon"),
                        monitors_quorum: find_u64!(s, "cluster.num_mon_quorum"),
                    })
                },
                None => None
            }
        }
    }

    pub fn log_to_influx(json: &String, client: &Client, hostname: &str) {
        match MonMeasurement::from_json(json) {
            Some(mon_m) => {
                let mut measurement = Measurement::new("mon_daemon");
                measurement.set_timestamp(time::now().to_timespec().sec);
                measurement.add_tag("type", "monitor");
                measurement.add_tag("hostname", hostname);

                measurement.add_field("used", Value::Integer(mon_m.used as i64));
                measurement.add_field("avail", Value::Integer(mon_m.avail as i64));
                measurement.add_field("total", Value::Integer(mon_m.total as i64));
                measurement.add_field("osds", Value::Integer(mon_m.osds as i64));
                measurement.add_field("osds_up", Value::Integer(mon_m.osds_up as i64));
                measurement.add_field("osds_in", Value::Integer(mon_m.osds_in as i64));
                measurement.add_field("osd_epoch", Value::Integer(mon_m.osd_epoch as i64));
                measurement.add_field("pgs", Value::Integer(mon_m.pgs as i64));
                measurement.add_field("pgs_active_clean", Value::Integer(mon_m.pgs_active_clean as i64));
                measurement.add_field("ppgs_active", Value::Integer(mon_m.ppgs_active as i64));
                measurement.add_field("pgs_peering", Value::Integer(mon_m.pgs_peering as i64));
                measurement.add_field("objects", Value::Integer(mon_m.objects as i64));
                measurement.add_field("objects_degraded", Value::Integer(mon_m.objects_degraded as i64));
                measurement.add_field("objects_unfound", Value::Integer(mon_m.objects_unfound as i64));
                measurement.add_field("monitors", Value::Integer(mon_m.monitors as i64));
                measurement.add_field("monitors_quorum", Value::Integer(mon_m.monitors_quorum as i64));

                // super::send_messages(vec![measurement], args);
                let _ = client.write_one(measurement, Some(Precision::Seconds));
            },
            None=> {}
        }

    }
}
