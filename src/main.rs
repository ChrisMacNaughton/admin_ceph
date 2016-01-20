extern crate ceph;
extern crate hyper;
#[macro_use] extern crate log;
extern crate output_args;
extern crate pcap;
extern crate regex;
extern crate rustc_serialize;
extern crate simple_logger;

mod messaging;
mod ceph_monitor;
mod ceph_osd;
mod ceph_packets;

fn version() -> String {
    format!("{}.{}.{}{}",
        env!("CARGO_PKG_VERSION_MAJOR"),
        env!("CARGO_PKG_VERSION_MINOR"),
        env!("CARGO_PKG_VERSION_PATCH"),
        option_env!("CARGO_PKG_VERSION_PRE").unwrap_or(""))
}

fn main() {
    println!("Starting program");
    let args = output_args::get_args("admin_ceph", &version());
    simple_logger::init_with_level(args.log_level).unwrap();
    info!("Logging with: {:?}", args);
    let output_sender = messaging::initialize_sender(args.clone());
    ceph_monitor::initialize_monitor_scanner(&output_sender);
    ceph_packets::initialize_pcap(&output_sender);
    ceph_osd::initialize_osd_scanner(&output_sender);
    loop {
        std::thread::sleep_ms(10000);
    }
}
