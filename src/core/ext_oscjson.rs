use log::{debug, info, warn};
use mdns_sd::{ServiceDaemon, ServiceEvent};
use rosc::{OscBundle, OscType};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    io::{Read, Write},
    rc::Rc,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use super::{bundle::AvatarBundle, folders::CONFIG_DIR};

const MAX_FETCH_RETRIES: u32 = 10;
const FETCH_RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub struct ExtOscJson {
    mdns: ServiceDaemon,
    mdns_recv: mdns_sd::Receiver<ServiceEvent>,
    oscjson_addr: Option<Arc<str>>,
    oscjson_name: Option<Arc<str>>,
    fetch_retry_at: Option<Instant>,
    fetch_retries_left: u32,
    client: reqwest::blocking::Client,
}

impl ExtOscJson {
    pub fn new() -> Self {
        let mdns = ServiceDaemon::new().unwrap();
        let mdns_recv = mdns.browse("_oscjson._tcp.local.").unwrap();
        let client = reqwest::blocking::Client::new();

        Self {
            mdns,
            mdns_recv,
            oscjson_addr: None,
            oscjson_name: None,
            fetch_retry_at: None,
            fetch_retries_left: 0,
            client,
        }
    }

    pub fn step(&mut self) -> bool {
        let mut notify_avatar = false;

        for event in self.mdns_recv.try_iter() {
            match event {
                ServiceEvent::ServiceResolved(info) => {
                    if !info.get_fullname().starts_with("VRChat-Client-") {
                        continue;
                    }
                    let Some(addr) = info.get_addresses_v4().into_iter().min() else {
                        warn!(
                            "OSCJSON service has no IPv4 address: {}",
                            info.get_fullname()
                        );
                        continue;
                    };
                    let url = format!("http://{}:{}/avatar", addr, info.get_port());

                    if self.oscjson_addr.as_deref() != Some(url.as_str()) {
                        info!(
                            "Found OSCJSON service: {} @ {}:{}",
                            info.get_fullname(),
                            addr,
                            info.get_port()
                        );
                        self.oscjson_addr = Some(url.into());
                        self.oscjson_name = Some(info.get_fullname().into());
                        self.fetch_retries_left = MAX_FETCH_RETRIES;
                        self.fetch_retry_at = None;
                        notify_avatar = true;
                    }
                }
                ServiceEvent::ServiceRemoved(_, fullname) => {
                    if self.oscjson_name.as_deref() == Some(fullname.as_str()) {
                        info!("Lost OSCJSON service: {}", fullname);
                        self.oscjson_addr = None;
                        self.oscjson_name = None;
                        self.fetch_retry_at = None;
                    }
                }
                _ => {}
            }
        }

        if self.fetch_retry_at.is_some_and(|at| at <= Instant::now()) {
            self.fetch_retry_at = None;
            notify_avatar = true;
        }

        notify_avatar
    }

    pub fn avatar(&mut self, avatar: &AvatarIdentifier) -> Option<OscJsonNode> {
        let node = self.fetch_avatar(avatar);

        // VRChat may advertise before its avatar tree is ready.
        self.fetch_retry_at = None;
        if matches!(avatar, AvatarIdentifier::Default)
            && !node.as_ref().is_some_and(|n| n.get("parameters").is_some())
            && self.fetch_retries_left > 0
        {
            self.fetch_retries_left -= 1;
            self.fetch_retry_at = Some(Instant::now() + FETCH_RETRY_INTERVAL);
        }

        node
    }

    fn fetch_avatar(&mut self, avatar: &AvatarIdentifier) -> Option<OscJsonNode> {
        let mut json = String::new();

        if let AvatarIdentifier::Path(path) = avatar {
            if let Err(e) = File::open(path.as_ref()).and_then(|mut f| f.read_to_string(&mut json))
            {
                log::error!("Could not read file: {:?}", e);
                return None;
            }
        } else {
            let Some(addr) = self.oscjson_addr.as_ref() else {
                warn!("No avatar oscjson address.");
                return None;
            };

            thread::sleep(Duration::from_millis(250));

            let Ok(resp) = self.client.get(addr.as_ref()).send() else {
                warn!("Failed to send avatar json request.");
                return None;
            };

            let Ok(text) = resp.text() else {
                warn!("No payload in avatar json response.");
                return None;
            };

            json = text;
        }

        let path = format!("{}/{}", CONFIG_DIR.as_ref(), "oscavmgr-avatar.json");
        if let Err(e) = File::create(path).and_then(|mut f| f.write_all(json.as_bytes())) {
            warn!("Could not write avatar json file: {:?}", e);
        }

        match serde_json::from_str(&json) {
            Ok(root_node) => Some(root_node),
            Err(e) => {
                warn!("Failed to deserialize avatar json: {}\n", e);
                debug!("{}", json);
                None
            }
        }
    }
}

#[derive(Debug)]
pub enum AvatarIdentifier {
    Default,
    Osc {
        uid: Rc<str>,
        name: Rc<str>,
        full_ft: bool,
    },
    Path(Rc<str>),
}

impl Display for AvatarIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "Default Avatar"),
            Self::Osc { uid, name, full_ft } => write!(
                f,
                "{} ({}) - {}",
                name.as_ref(),
                uid.as_ref(),
                if *full_ft { "Native FT" } else { "Legacy FT" }
            ),
            Self::Path(path) => write!(f, "Avatar from file: {}", path.as_ref()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OscJsonNode {
    #[serde(alias = "FULL_PATH")]
    pub full_path: Arc<str>,
    #[serde(alias = "ACCESS")]
    pub access: i32,
    #[serde(alias = "TYPE")]
    pub data_type: Option<Arc<str>>,
    #[serde(alias = "CONTENTS")]
    pub contents: Option<HashMap<Arc<str>, OscJsonNode>>,
    #[serde(alias = "VALUE")]
    pub value: Option<Vec<serde_json::Value>>,
}

impl OscJsonNode {
    pub fn get(&self, path: &str) -> Option<&OscJsonNode> {
        let mut node = self;
        for part in path.split('/') {
            if let Some(contents) = &node.contents {
                node = contents.get(part)?;
            } else {
                return None;
            }
        }
        Some(node)
    }

    pub fn has_vsync(&self) -> bool {
        self.get("parameters")
            .and_then(|parameters| parameters.get("VSync"))
            .is_some()
    }
}

#[derive(Clone)]
pub struct MysteryParam {
    pub name: Arc<str>,
    pub main_address: Option<Arc<str>>,
    pub addresses: [Option<Arc<str>>; 7],
    pub neg_address: Option<Arc<str>>,
    pub num_bits: usize,
    pub last_value: f32,
    pub last_bits: [bool; 8],
}

impl MysteryParam {
    pub fn send(&mut self, value: f32, bundle: &mut OscBundle) {
        if let Some(addr) = self.main_address.as_ref() {
            if (value - self.last_value).abs() > 0.01 {
                bundle.send_parameter(addr, OscType::Float(value));
                self.last_value = value;
            }
        }

        let mut value = value;
        if let Some(addr) = self.neg_address.as_ref() {
            let send_val = value < 0.;
            if self.last_bits[7] != send_val {
                bundle.send_parameter(addr, OscType::Bool(send_val));
                self.last_bits[7] = send_val;
            }
            value = value.abs();
        } else if value < 0. {
            value = 0.;
        }

        let value = (value * ((1 << self.num_bits) - 1) as f32) as i32;

        self.addresses
            .iter()
            .enumerate()
            .take(self.num_bits)
            .for_each(|(idx, param)| {
                if let Some(addr) = param.as_ref() {
                    let send_val = value & (1 << idx) != 0;
                    if self.last_bits[idx] != send_val {
                        bundle.send_parameter(addr, OscType::Bool(send_val));
                        self.last_bits[idx] = send_val;
                    }
                }
            });
    }
}
