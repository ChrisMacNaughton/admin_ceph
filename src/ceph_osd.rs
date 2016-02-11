use std::fs;
use std::io;
use std::path::Path;
use std::str::FromStr;
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::Duration;

use ceph::{osd_mount_point, get_osd_perf_dump_raw};
use regex::Regex;

use Args;
use influent::create_client;
use influent::client::Credentials;

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

pub fn initialize_osd_scanner(args: &Args) {
    let args = args.clone();

    thread::spawn(move || {
        let do_influx = args.influx.is_some() && args.outputs.contains(&"influx".to_string());
        let mut user: String = String::new();
        let mut password: String = String::new();
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
                        // let _ = log_queue.send(LogMessage{ log_type: LogType::CephDaemonOsdMessage, json_body: osd, osd_num: Some(*osd_num), drive_name: Some(drive_name)});
                        let osd_num = format!("{}", osd_num);
                        if do_influx {
                            influx::send_to_influx(&osd, &client, &args.hostname, &drive_name[..], &osd_num[..]);
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

mod influx {
    use time;
    use influent::measurement::{Measurement, Value};
    use influent::client::{Precision, Client};
    use serde_json;

    macro_rules! find_u64 {
        ($container:ident, $name:expr) => {
            match $container.lookup($name) {
                Some(s) => {
                    s.as_u64().unwrap_or(0)
                }, None => 0
            }
        }
    }

    macro_rules! find_f64 {
        ($container:ident, $name:expr) => {
            match $container.lookup($name) {
                Some(s) => {
                    s.as_f64().unwrap_or(0.0)
                }, None => 0.0
            }
        }
    }

    macro_rules! find_string {
        ($container:ident, $name:expr) => {
            match $container.lookup($name) {
                Some(s) => {
                    let st: Option<&str> = s.as_string();
                     match st {
                        Some(s) => s.to_string(),
                        None => String::new(),
                     }
                }, None => String::new()
            }
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_osd_measurement_json() {
            let json = r#"{
                "WBThrottle":{"bytes_dirtied":58720256,"bytes_wb":6530531328,"ios_dirtied":14,"ios_wb":1557,"inodes_dirtied":14,"inodes_wb":1557},"filestore":{"journal_queue_max_ops":300,"journal_queue_ops":0,"journal_ops":4889,"journal_queue_max_bytes":33554432,"journal_queue_bytes":0,"journal_bytes":7840178464,"journal_latency":{"avgcount":4889,"sum":1532.535021123},"journal_wr":1604,"journal_wr_bytes":{"avgcount":1604,"sum":7859097600},"journal_full":0,"committing":1,"commitcycle":31,"commitcycle_interval":{"avgcount":31,"sum":272.424822088},"commitcycle_latency":{"avgcount":31,"sum":117.046209984},"op_queue_max_ops":50,"op_queue_ops":0,"ops":4889,"op_queue_max_bytes":104857600,"op_queue_bytes":0,"bytes":7840137930,"apply_latency":{"avgcount":4889,"sum":1947.011337099},"queue_transaction_latency_avg":{"avgcount":4889,"sum":1.801399483}},"leveldb":{"leveldb_get":1843,"leveldb_transaction":12378,"leveldb_compact":0,"leveldb_compact_range":0,"leveldb_compact_queue_merge":0,"leveldb_compact_queue_len":0},"mutex-FileJournal::completions_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-FileJournal::finisher_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-FileJournal::write_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-FileJournal::writeq_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-JOS::ApplyManager::apply_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-JOS::ApplyManager::com_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-JOS::SubmitManager::lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-WBThrottle::lock":{"wait":{"avgcount":0,"sum":0.0}},"objecter":{"op_active":0,"op_laggy":0,"op_send":0,"op_send_bytes":0,"op_resend":0,"op_ack":0,"op_commit":0,"op":0,"op_r":0,"op_w":0,"op_rmw":0,"op_pg":0,"osdop_stat":0,"osdop_create":0,"osdop_read":0,"osdop_write":0,"osdop_writefull":0,"osdop_append":0,"osdop_zero":0,"osdop_truncate":0,"osdop_delete":0,"osdop_mapext":0,"osdop_sparse_read":0,"osdop_clonerange":0,"osdop_getxattr":0,"osdop_setxattr":0,"osdop_cmpxattr":0,"osdop_rmxattr":0,"osdop_resetxattrs":0,"osdop_tmap_up":0,"osdop_tmap_put":0,"osdop_tmap_get":0,"osdop_call":0,"osdop_watch":0,"osdop_notify":0,"osdop_src_cmpxattr":0,"osdop_pgls":0,"osdop_pgls_filter":0,"osdop_other":0,"linger_active":0,"linger_send":0,"linger_resend":0,"linger_ping":0,"poolop_active":0,"poolop_send":0,"poolop_resend":0,"poolstat_active":0,"poolstat_send":0,"poolstat_resend":0,"statfs_active":0,"statfs_send":0,"statfs_resend":0,"command_active":0,"command_send":0,"command_resend":0,"map_epoch":213,"map_full":0,"map_inc":9,"osd_sessions":0,"osd_session_open":0,"osd_session_close":0,"osd_laggy":0},"osd":{"op_wip":0,"op":642,"op_in_bytes":1451229184,"op_out_bytes":0,"op_latency":{"avgcount":642,"sum":379.868778082},"op_process_latency":{"avgcount":642,"sum":370.393252306},"op_r":0,"op_r_out_bytes":0,"op_r_latency":{"avgcount":0,"sum":0.0},"op_r_process_latency":{"avgcount":0,"sum":0.0},"op_w":642,"op_w_in_bytes":1451229184,"op_w_rlat":{"avgcount":308,"sum":102.256953393},"op_w_latency":{"avgcount":642,"sum":379.868778082},"op_w_process_latency":{"avgcount":642,"sum":370.393252306},"op_rw":0,"op_rw_in_bytes":0,"op_rw_out_bytes":0,"op_rw_rlat":{"avgcount":0,"sum":0.0},"op_rw_latency":{"avgcount":0,"sum":0.0},"op_rw_process_latency":{"avgcount":0,"sum":0.0},"subop":2847,"subop_in_bytes":6380867868,"subop_latency":{"avgcount":2847,"sum":1493.794261123},"subop_w":2847,"subop_w_in_bytes":6380867868,"subop_w_latency":{"avgcount":2847,"sum":1493.794261123},"subop_pull":0,"subop_pull_latency":{"avgcount":0,"sum":0.0},"subop_push":0,"subop_push_in_bytes":0,"subop_push_latency":{"avgcount":0,"sum":0.0},"pull":0,"push":0,"push_out_bytes":0,"push_in":0,"push_in_bytes":0,"recovery_ops":0,"loadavg":1853,"buffer_bytes":0,"numpg":162,"numpg_primary":57,"numpg_replica":105,"numpg_stray":0,"heartbeat_to_peers":4,"heartbeat_from_peers":0,"map_messages":33,"map_message_epochs":69,"map_message_epoch_dups":51,"messages_delayed_for_map":0,"stat_bytes":466472001536,"stat_bytes_used":176487571456,"stat_bytes_avail":266265432064,"copyfrom":0,"tier_promote":0,"tier_flush":0,"tier_flush_fail":0,"tier_try_flush":0,"tier_try_flush_fail":0,"tier_evict":0,"tier_whiteout":0,"tier_dirty":346,"tier_clean":0,"tier_delay":0,"tier_proxy_read":0,"agent_wake":0,"agent_skip":0,"agent_flush":0,"agent_evict":0,"object_ctx_cache_hit":296,"object_ctx_cache_total":988},"recoverystate_perf":{"initial_latency":{"avgcount":224,"sum":0.032990481},"started_latency":{"avgcount":216,"sum":172.258483651},"reset_latency":{"avgcount":440,"sum":16.299056885},"start_latency":{"avgcount":440,"sum":0.042277681},"primary_latency":{"avgcount":33,"sum":6.452393779},"peering_latency":{"avgcount":90,"sum":63.484997863},"backfilling_latency":{"avgcount":0,"sum":0.0},"waitremotebackfillreserved_latency":{"avgcount":0,"sum":0.0},"waitlocalbackfillreserved_latency":{"avgcount":0,"sum":0.0},"notbackfilling_latency":{"avgcount":0,"sum":0.0},"repnotrecovering_latency":{"avgcount":63,"sum":75.246449242},"repwaitrecoveryreserved_latency":{"avgcount":1,"sum":0.027250331},"repwaitbackfillreserved_latency":{"avgcount":0,"sum":0.0},"RepRecovering_latency":{"avgcount":1,"sum":0.02869367},"activating_latency":{"avgcount":57,"sum":22.301761664},"waitlocalrecoveryreserved_latency":{"avgcount":0,"sum":0.0},"waitremoterecoveryreserved_latency":{"avgcount":0,"sum":0.0},"recovering_latency":{"avgcount":0,"sum":0.0},"recovered_latency":{"avgcount":57,"sum":0.002819468},"clean_latency":{"avgcount":0,"sum":0.0},"active_latency":{"avgcount":0,"sum":0.0},"replicaactive_latency":{"avgcount":62,"sum":75.303835338},"stray_latency":{"avgcount":288,"sum":216.002745247},"getinfo_latency":{"avgcount":90,"sum":17.852408456},"getlog_latency":{"avgcount":57,"sum":3.412500086},"waitactingchange_latency":{"avgcount":0,"sum":0.0},"incomplete_latency":{"avgcount":0,"sum":0.0},"getmissing_latency":{"avgcount":57,"sum":0.001258674},"waitupthru_latency":{"avgcount":57,"sum":42.215188461}},"throttle-filestore_bytes":{"val":0,"max":33554432,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":4889,"take_sum":7840178464,"put":1601,"put_sum":7840178464,"wait":{"avgcount":915,"sum":254.789725891}},"throttle-filestore_ops":{"val":0,"max":300,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":4889,"take_sum":4889,"put":1601,"put_sum":4889,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-client":{"val":0,"max":104857600,"get":831,"get_sum":1451436395,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":831,"put_sum":1451436395,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-cluster":{"val":0,"max":104857600,"get":5157,"get_sum":6386736929,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":5157,"put_sum":6386736929,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-hb_back_server":{"val":0,"max":104857600,"get":11969,"get_sum":562543,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":11969,"put_sum":562543,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-hb_front_server":{"val":0,"max":104857600,"get":11969,"get_sum":562543,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":11969,"put_sum":562543,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-hbclient":{"val":0,"max":104857600,"get":19686,"get_sum":925242,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":19686,"put_sum":925242,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-ms_objecter":{"val":0,"max":104857600,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":0,"put_sum":0,"wait":{"avgcount":0,"sum":0.0}},"throttle-objecter_bytes":{"val":0,"max":104857600,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":0,"put_sum":0,"wait":{"avgcount":0,"sum":0.0}},"throttle-objecter_ops":{"val":0,"max":1024,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":0,"put_sum":0,"wait":{"avgcount":0,"sum":0.0}},"throttle-osd_client_bytes":{"val":0,"max":524288000,"get":642,"get_sum":1451364542,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":988,"put_sum":1451364542,"wait":{"avgcount":0,"sum":0.0}},"throttle-osd_client_messages":{"val":5,"max":100,"get":642,"get_sum":642,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":637,"put_sum":637,"wait":{"avgcount":0,"sum":0.0}},"measurement":"osd","osd_num":"3","drive_name":"","hostname":"chris-local-machine-9","controller":"ceph_measurement","action":"ceph_message","ceph_measurement":{"WBThrottle":{"bytes_dirtied":58720256,"bytes_wb":6530531328,"ios_dirtied":14,"ios_wb":1557,"inodes_dirtied":14,"inodes_wb":1557},"filestore":{"journal_queue_max_ops":300,"journal_queue_ops":0,"journal_ops":4889,"journal_queue_max_bytes":33554432,"journal_queue_bytes":0,"journal_bytes":7840178464,"journal_latency":{"avgcount":4889,"sum":1532.535021123},"journal_wr":1604,"journal_wr_bytes":{"avgcount":1604,"sum":7859097600},"journal_full":0,"committing":1,"commitcycle":31,"commitcycle_interval":{"avgcount":31,"sum":272.424822088},"commitcycle_latency":{"avgcount":31,"sum":117.046209984},"op_queue_max_ops":50,"op_queue_ops":0,"ops":4889,"op_queue_max_bytes":104857600,"op_queue_bytes":0,"bytes":7840137930,"apply_latency":{"avgcount":4889,"sum":1947.011337099},"queue_transaction_latency_avg":{"avgcount":4889,"sum":1.801399483}},"leveldb":{"leveldb_get":1843,"leveldb_transaction":12378,"leveldb_compact":0,"leveldb_compact_range":0,"leveldb_compact_queue_merge":0,"leveldb_compact_queue_len":0},"mutex-FileJournal::completions_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-FileJournal::finisher_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-FileJournal::write_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-FileJournal::writeq_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-JOS::ApplyManager::apply_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-JOS::ApplyManager::com_lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-JOS::SubmitManager::lock":{"wait":{"avgcount":0,"sum":0.0}},"mutex-WBThrottle::lock":{"wait":{"avgcount":0,"sum":0.0}},"objecter":{"op_active":0,"op_laggy":0,"op_send":0,"op_send_bytes":0,"op_resend":0,"op_ack":0,"op_commit":0,"op":0,"op_r":0,"op_w":0,"op_rmw":0,"op_pg":0,"osdop_stat":0,"osdop_create":0,"osdop_read":0,"osdop_write":0,"osdop_writefull":0,"osdop_append":0,"osdop_zero":0,"osdop_truncate":0,"osdop_delete":0,"osdop_mapext":0,"osdop_sparse_read":0,"osdop_clonerange":0,"osdop_getxattr":0,"osdop_setxattr":0,"osdop_cmpxattr":0,"osdop_rmxattr":0,"osdop_resetxattrs":0,"osdop_tmap_up":0,"osdop_tmap_put":0,"osdop_tmap_get":0,"osdop_call":0,"osdop_watch":0,"osdop_notify":0,"osdop_src_cmpxattr":0,"osdop_pgls":0,"osdop_pgls_filter":0,"osdop_other":0,"linger_active":0,"linger_send":0,"linger_resend":0,"linger_ping":0,"poolop_active":0,"poolop_send":0,"poolop_resend":0,"poolstat_active":0,"poolstat_send":0,"poolstat_resend":0,"statfs_active":0,"statfs_send":0,"statfs_resend":0,"command_active":0,"command_send":0,"command_resend":0,"map_epoch":213,"map_full":0,"map_inc":9,"osd_sessions":0,"osd_session_open":0,"osd_session_close":0,"osd_laggy":0},"osd":{"op_wip":0,"op":642,"op_in_bytes":1451229184,"op_out_bytes":0,"op_latency":{"avgcount":642,"sum":379.868778082},"op_process_latency":{"avgcount":642,"sum":370.393252306},"op_r":0,"op_r_out_bytes":0,"op_r_latency":{"avgcount":0,"sum":0.0},"op_r_process_latency":{"avgcount":0,"sum":0.0},"op_w":642,"op_w_in_bytes":1451229184,"op_w_rlat":{"avgcount":308,"sum":102.256953393},"op_w_latency":{"avgcount":642,"sum":379.868778082},"op_w_process_latency":{"avgcount":642,"sum":370.393252306},"op_rw":0,"op_rw_in_bytes":0,"op_rw_out_bytes":0,"op_rw_rlat":{"avgcount":0,"sum":0.0},"op_rw_latency":{"avgcount":0,"sum":0.0},"op_rw_process_latency":{"avgcount":0,"sum":0.0},"subop":2847,"subop_in_bytes":6380867868,"subop_latency":{"avgcount":2847,"sum":1493.794261123},"subop_w":2847,"subop_w_in_bytes":6380867868,"subop_w_latency":{"avgcount":2847,"sum":1493.794261123},"subop_pull":0,"subop_pull_latency":{"avgcount":0,"sum":0.0},"subop_push":0,"subop_push_in_bytes":0,"subop_push_latency":{"avgcount":0,"sum":0.0},"pull":0,"push":0,"push_out_bytes":0,"push_in":0,"push_in_bytes":0,"recovery_ops":0,"loadavg":1853,"buffer_bytes":0,"numpg":162,"numpg_primary":57,"numpg_replica":105,"numpg_stray":0,"heartbeat_to_peers":4,"heartbeat_from_peers":0,"map_messages":33,"map_message_epochs":69,"map_message_epoch_dups":51,"messages_delayed_for_map":0,"stat_bytes":466472001536,"stat_bytes_used":176487571456,"stat_bytes_avail":266265432064,"copyfrom":0,"tier_promote":0,"tier_flush":0,"tier_flush_fail":0,"tier_try_flush":0,"tier_try_flush_fail":0,"tier_evict":0,"tier_whiteout":0,"tier_dirty":346,"tier_clean":0,"tier_delay":0,"tier_proxy_read":0,"agent_wake":0,"agent_skip":0,"agent_flush":0,"agent_evict":0,"object_ctx_cache_hit":296,"object_ctx_cache_total":988},"recoverystate_perf":{"initial_latency":{"avgcount":224,"sum":0.032990481},"started_latency":{"avgcount":216,"sum":172.258483651},"reset_latency":{"avgcount":440,"sum":16.299056885},"start_latency":{"avgcount":440,"sum":0.042277681},"primary_latency":{"avgcount":33,"sum":6.452393779},"peering_latency":{"avgcount":90,"sum":63.484997863},"backfilling_latency":{"avgcount":0,"sum":0.0},"waitremotebackfillreserved_latency":{"avgcount":0,"sum":0.0},"waitlocalbackfillreserved_latency":{"avgcount":0,"sum":0.0},"notbackfilling_latency":{"avgcount":0,"sum":0.0},"repnotrecovering_latency":{"avgcount":63,"sum":75.246449242},"repwaitrecoveryreserved_latency":{"avgcount":1,"sum":0.027250331},"repwaitbackfillreserved_latency":{"avgcount":0,"sum":0.0},"RepRecovering_latency":{"avgcount":1,"sum":0.02869367},"activating_latency":{"avgcount":57,"sum":22.301761664},"waitlocalrecoveryreserved_latency":{"avgcount":0,"sum":0.0},"waitremoterecoveryreserved_latency":{"avgcount":0,"sum":0.0},"recovering_latency":{"avgcount":0,"sum":0.0},"recovered_latency":{"avgcount":57,"sum":0.002819468},"clean_latency":{"avgcount":0,"sum":0.0},"active_latency":{"avgcount":0,"sum":0.0},"replicaactive_latency":{"avgcount":62,"sum":75.303835338},"stray_latency":{"avgcount":288,"sum":216.002745247},"getinfo_latency":{"avgcount":90,"sum":17.852408456},"getlog_latency":{"avgcount":57,"sum":3.412500086},"waitactingchange_latency":{"avgcount":0,"sum":0.0},"incomplete_latency":{"avgcount":0,"sum":0.0},"getmissing_latency":{"avgcount":57,"sum":0.001258674},"waitupthru_latency":{"avgcount":57,"sum":42.215188461}},"throttle-filestore_bytes":{"val":0,"max":33554432,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":4889,"take_sum":7840178464,"put":1601,"put_sum":7840178464,"wait":{"avgcount":915,"sum":254.789725891}},"throttle-filestore_ops":{"val":0,"max":300,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":4889,"take_sum":4889,"put":1601,"put_sum":4889,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-client":{"val":0,"max":104857600,"get":831,"get_sum":1451436395,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":831,"put_sum":1451436395,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-cluster":{"val":0,"max":104857600,"get":5157,"get_sum":6386736929,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":5157,"put_sum":6386736929,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-hb_back_server":{"val":0,"max":104857600,"get":11969,"get_sum":562543,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":11969,"put_sum":562543,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-hb_front_server":{"val":0,"max":104857600,"get":11969,"get_sum":562543,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":11969,"put_sum":562543,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-hbclient":{"val":0,"max":104857600,"get":19686,"get_sum":925242,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":19686,"put_sum":925242,"wait":{"avgcount":0,"sum":0.0}},"throttle-msgr_dispatch_throttler-ms_objecter":{"val":0,"max":104857600,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":0,"put_sum":0,"wait":{"avgcount":0,"sum":0.0}},"throttle-objecter_bytes":{"val":0,"max":104857600,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":0,"put_sum":0,"wait":{"avgcount":0,"sum":0.0}},"throttle-objecter_ops":{"val":0,"max":1024,"get":0,"get_sum":0,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":0,"put_sum":0,"wait":{"avgcount":0,"sum":0.0}},"throttle-osd_client_bytes":{"val":0,"max":524288000,"get":642,"get_sum":1451364542,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":988,"put_sum":1451364542,"wait":{"avgcount":0,"sum":0.0}},"throttle-osd_client_messages":{"val":5,"max":100,"get":642,"get_sum":642,"get_or_fail_fail":0,"get_or_fail_success":0,"take":0,"take_sum":0,"put":637,"put_sum":637,"wait":{"avgcount":0,"sum":0.0}}}
                }"#.to_string();
                let measurement = super::OsdMeasurement::from_json(&json).unwrap();

                assert_eq!(measurement.ops, 4889);
        }
    }

    struct OsdMeasurement {
        load_average: u64,
        queued_ops: u64,
        stat_bytes: u64,
        stat_bytes_used: u64,
        stat_bytes_avail: u64,
        op_latency: f64,
        op_r_latency: f64,
        op_w_latency: f64,
        subop_latency: f64,
        subop_w_latency: f64,
        journal_latency: f64,
        apply_latency: f64,
        commit_latency: f64,
        queue_transaction_latency_avg: f64,
        ops: u64,
    }

    impl OsdMeasurement {
        pub fn from_json(json_string: &String) -> Option<OsdMeasurement> {
            let value: Option<serde_json::Value> = match serde_json::from_str(&json_string[..]){
                Ok(s) => Some(s),
                Err(e) => {
                    info!("Problem parsing json: {:?}", e);
                    None
                }
            };

            match value {
                Some(s) => {
                    Some(OsdMeasurement {
                        load_average: find_u64!(s, "osd.loadavg"),
                        queued_ops: find_u64!(s, "filestore.op_queue_ops"),
                        stat_bytes: find_u64!(s, "osd.stat_bytes"),
                        stat_bytes_used: find_u64!(s, "osd.stat_bytes_used"),
                        stat_bytes_avail: find_u64!(s, "osd.stat_bytes_avail"),
                        op_latency: find_f64!(s, "osd.op_latency.sum"),
                        op_r_latency: find_f64!(s, "osd.op_r_latency.sum"),
                        op_w_latency: find_f64!(s, "osd.op_w_latency.sum"),
                        subop_latency: find_f64!(s, "osd.subop_latency.sum"),
                        subop_w_latency: find_f64!(s, "osd.subop_w_latency.sum"),
                        journal_latency: find_f64!(s, "filestore.journal_latency.sum"),
                        apply_latency: find_f64!(s, "filestore.apply_latency.sum"),
                        commit_latency: find_f64!(s, "filestore.commit_latency.sum"),
                        queue_transaction_latency_avg: find_f64!(s, "filestore.queue_transaction_latency_avg.sum"),
                        ops: find_u64!(s, "filestore.ops"),
                    })
                },
                None => None
            }
        }
    }

    pub fn send_to_influx(json: &String, client: &Client, hostname: &str, drive_name: &str, osd_num: &str) {
        match OsdMeasurement::from_json(json) {
            Some(osd_m) => {
                let mut measurement = Measurement::new("osd_daemon");
                measurement.add_tag("type", "osd");
                measurement.set_timestamp(time::now().to_timespec().sec as i32);
                measurement.add_tag("hostname", hostname);
                measurement.add_tag("osd_num", osd_num);
                measurement.add_tag("drive_name", drive_name);

                measurement.add_field("load_average", Value::Integer(osd_m.load_average as i64));
                measurement.add_field("queued_ops", Value::Integer(osd_m.queued_ops as i64));
                measurement.add_field("stat_bytes", Value::Integer(osd_m.stat_bytes as i64));
                measurement.add_field("stat_bytes_used", Value::Integer(osd_m.stat_bytes_used as i64));
                measurement.add_field("stat_bytes_avail", Value::Integer(osd_m.stat_bytes_avail as i64));
                measurement.add_field("op_latency", Value::Float(osd_m.op_latency as f64));
                measurement.add_field("op_r_latency", Value::Float(osd_m.op_r_latency as f64));
                measurement.add_field("op_w_latency", Value::Float(osd_m.op_w_latency as f64));
                measurement.add_field("subop_latency", Value::Float(osd_m.subop_latency as f64));
                measurement.add_field("subop_w_latency", Value::Float(osd_m.subop_w_latency as f64));
                measurement.add_field("journal_latency", Value::Float(osd_m.journal_latency as f64));
                measurement.add_field("apply_latency", Value::Float(osd_m.apply_latency as f64));
                measurement.add_field("commit_latency", Value::Float(osd_m.commit_latency as f64));
                measurement.add_field("queue_transaction_latency_avg", Value::Float(osd_m.queue_transaction_latency_avg as f64));
                measurement.add_field("ops", Value::Integer(osd_m.ops as i64));

                let _ = client.write_one(measurement, Some(Precision::Seconds));
            },
            None=> {}
        };

    }
}
