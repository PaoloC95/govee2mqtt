//! Govee Platform API MQTT event subscriber. Devices that advertise
//! `devices.capabilities.event` (presence sensors, ice makers, dehumidifiers,
//! ...) publish notifications to a broker at mqtts://mqtt.openapi.govee.com:8883
//! that is separate from the AWS IoT broker the undocumented API uses.
//!
//! Protocol summary, from <https://developer.govee.com/reference/subscribe-device-event>:
//!  - TLS (public CA), username + password are both the API key
//!  - Topic is documented as `GA/<api-key>` in prose but the JS example in the
//!    same page subscribes to just `<api-key>`; we subscribe to both
//!  - Payload: `{sku, device, deviceName, capabilities: [{type, instance,
//!    state: [{name, value, message?}]}]}`
//!
//! For now we just decode and log the events and signal a state-change so any
//! HTTP-API state subscribers re-query. Mapping event-instance values into
//! per-device state belongs in the device modules, where the schema is known.

use crate::service::state::StateHandle;
use anyhow::Context;
use govee_api::http::from_json;
use govee_api::platform_api::DeviceCapabilityKind;
use rumqttc::{
    AsyncClient, Event, EventLoop, MqttOptions, NetworkOptions, Packet as MqttPacket, QoS,
    Transport,
};
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, lookup_host};
use tokio_rustls::TlsConnector;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::time::Duration;

const BROKER_HOST: &str = "mqtt.openapi.govee.com";
const BROKER_PORT: u16 = 8883;
// rumqttc's default is 5 seconds. Give the TLS + MQTT handshake more time while
// diagnosing the Platform broker's repeated NetworkTimeout responses.
const CONNECTION_TIMEOUT_SECS: u64 = 30;
const PROBE_STAGE_TIMEOUT_SECS: u64 = 12;
const PROBE_RETRY_SECS: u64 = 15;

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct EventMessage {
    sku: String,
    device: String,
    #[serde(default, rename = "deviceName")]
    device_name: String,
    #[serde(default)]
    capabilities: Vec<EventCapability>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct EventCapability {
    #[serde(rename = "type")]
    kind: DeviceCapabilityKind,
    instance: String,
    #[serde(default)]
    state: Vec<EventState>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct EventState {
    name: String,
    #[serde(default)]
    value: JsonValue,
    #[serde(default)]
    message: Option<String>,
}



/// Independently exercise each stage of the Platform MQTT connection so a
/// generic rumqttc NetworkTimeout can be narrowed to DNS, TCP, TLS, CONNECT,
/// or CONNACK. Credentials are sent only over the verified TLS stream and
/// are never included in diagnostic output.
async fn probe_platform_mqtt(api_key: &str) -> anyhow::Result<()> {
    log::info!("Platform MQTT diagnostic: resolving {BROKER_HOST}:{BROKER_PORT}");
    let addresses: Vec<SocketAddr> = lookup_host((BROKER_HOST, BROKER_PORT))
        .await
        .context("DNS lookup failed for Platform MQTT broker")?
        .collect();
    let address = addresses
        .first()
        .copied()
        .context("DNS lookup returned no Platform MQTT addresses")?;
    log::info!("Platform MQTT diagnostic: DNS resolved to {address}");

    log::info!("Platform MQTT diagnostic: opening TCP connection");
    let tcp = tokio::time::timeout(
        Duration::from_secs(PROBE_STAGE_TIMEOUT_SECS),
        TcpStream::connect(address),
    )
    .await
    .context("TCP connection timed out")?
    .context("TCP connection failed")?;
    log::info!("Platform MQTT diagnostic: TCP connected");

    log::info!("Platform MQTT diagnostic: loading system CA certificates");
    let native_certs = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    let mut accepted = 0usize;
    for cert in native_certs.certs {
        if roots.add(cert).is_ok() {
            accepted += 1;
        }
    }
    if accepted == 0 {
        anyhow::bail!(
            "TLS setup failed: no usable system CA certificates ({} load errors)",
            native_certs.errors.len()
        );
    }
    log::info!(
        "Platform MQTT diagnostic: loaded {accepted} system CA certificates ({} load errors)",
        native_certs.errors.len()
    );

    let tls_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_config));
    let server_name = ServerName::try_from(BROKER_HOST.to_owned())
        .context("TLS server name is invalid")?;

    log::info!("Platform MQTT diagnostic: starting TLS handshake");
    let mut tls = tokio::time::timeout(
        Duration::from_secs(PROBE_STAGE_TIMEOUT_SECS),
        connector.connect(server_name, tcp),
    )
    .await
    .context("TLS handshake timed out")?
    .context("TLS handshake failed")?;
    log::info!("Platform MQTT diagnostic: TLS handshake completed");

    let client_id = format!("govee2mqtt-probe-{}", uuid::Uuid::new_v4().simple());
    let packet = mqtt_connect_packet(&client_id, api_key);
    log::info!("Platform MQTT diagnostic: sending MQTT 3.1.1 CONNECT");
    tokio::time::timeout(
        Duration::from_secs(PROBE_STAGE_TIMEOUT_SECS),
        tls.write_all(&packet),
    )
    .await
    .context("MQTT CONNECT write timed out")?
    .context("MQTT CONNECT write failed")?;
    tls.flush().await.context("MQTT CONNECT flush failed")?;

    log::info!("Platform MQTT diagnostic: waiting for broker CONNACK");
    let mut header = [0u8; 1];
    tokio::time::timeout(
        Duration::from_secs(PROBE_STAGE_TIMEOUT_SECS),
        tls.read_exact(&mut header),
    )
    .await
    .context("Broker did not answer MQTT CONNECT: CONNACK wait timed out")?
    .context("Failed while reading MQTT response header")?;

    let mut remaining_length = 0usize;
    let mut multiplier = 1usize;
    let mut encoded = [0u8; 1];
    for _ in 0..4 {
        tokio::time::timeout(
            Duration::from_secs(PROBE_STAGE_TIMEOUT_SECS),
            tls.read_exact(&mut encoded),
        )
        .await
        .context("Timed out reading MQTT response length")?
        .context("Failed while reading MQTT response length")?;
        remaining_length += ((encoded[0] & 0x7f) as usize) * multiplier;
        if encoded[0] & 0x80 == 0 {
            break;
        }
        multiplier = multiplier.saturating_mul(128);
    }
    if remaining_length == 0 || remaining_length > 1024 {
        anyhow::bail!(
            "Unexpected MQTT response: packet type 0x{:02x}, remaining length {remaining_length}",
            header[0] >> 4
        );
    }
    let mut body = vec![0u8; remaining_length];
    tokio::time::timeout(
        Duration::from_secs(PROBE_STAGE_TIMEOUT_SECS),
        tls.read_exact(&mut body),
    )
    .await
    .context("Timed out reading MQTT response body")?
    .context("Failed while reading MQTT response body")?;

    if header[0] >> 4 != 2 || body.len() < 2 {
        anyhow::bail!(
            "Expected MQTT CONNACK, received packet type 0x{:02x}",
            header[0] >> 4
        );
    }
    let return_code = body[1];
    if return_code != 0 {
        anyhow::bail!(
            "Govee broker rejected MQTT CONNECT with CONNACK return code {return_code}"
        );
    }
    log::info!("Platform MQTT diagnostic: broker accepted MQTT CONNECT (CONNACK 0)");
    Ok(())
}

fn mqtt_connect_packet(client_id: &str, api_key: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0xC2, 0x00, 0x3C]);
    push_mqtt_string(&mut body, client_id);
    push_mqtt_string(&mut body, api_key);
    push_mqtt_string(&mut body, api_key);

    let mut packet = vec![0x10];
    let mut remaining = body.len();
    loop {
        let mut digit = (remaining % 128) as u8;
        remaining /= 128;
        if remaining > 0 {
            digit |= 0x80;
        }
        packet.push(digit);
        if remaining == 0 {
            break;
        }
    }
    packet.extend_from_slice(&body);
    packet
}

fn push_mqtt_string(buffer: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    buffer.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buffer.extend_from_slice(bytes);
}

pub async fn start_platform_iot(api_key: String, state: StateHandle) -> anyhow::Result<()> {
    // client_id has to be unique per connection; the broker is EMQX-backed and
    // disconnects duplicates.
    let client_id = format!("govee2mqtt-{}", uuid::Uuid::new_v4().simple());

    let mut mqtt_options = MqttOptions::new(client_id, BROKER_HOST, BROKER_PORT);
    mqtt_options.set_credentials(&api_key, &api_key);
    mqtt_options.set_keep_alive(Duration::from_secs(60));
    mqtt_options.set_transport(Transport::tls_with_default_config());

    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 32);
    let mut network_options = NetworkOptions::new();
    network_options.set_connection_timeout(CONNECTION_TIMEOUT_SECS);
    eventloop.set_network_options(network_options);

    log::info!(
        "Connecting to platform MQTT {BROKER_HOST}:{BROKER_PORT} (connection timeout: {CONNECTION_TIMEOUT_SECS}s)"
    );
    tokio::spawn(async move {
        loop {
            match probe_platform_mqtt(&api_key).await {
                Ok(()) => {
                    log::info!(
                        "Platform MQTT diagnostic succeeded; starting rumqttc client"
                    );
                    break;
                }
                Err(err) => {
                    log::warn!("Platform MQTT diagnostic failed: {err:#}");
                    log::info!(
                        "Retrying Platform MQTT diagnostic in {PROBE_RETRY_SECS}s"
                    );
                    tokio::time::sleep(Duration::from_secs(PROBE_RETRY_SECS)).await;
                }
            }
        }

        if let Err(err) = run_platform_iot(eventloop, state, client, api_key).await {
            log::error!("Platform MQTT loop failed: {err:#}");
        }
        log::info!("Platform MQTT loop terminated");
    });

    Ok(())
}

async fn run_platform_iot(
    mut eventloop: EventLoop,
    state: StateHandle,
    client: AsyncClient,
    api_key: String,
) -> anyhow::Result<()> {
    loop {
        let event = match eventloop.poll().await {
            Ok(event) => event,
            Err(err) => {
                // rumqttc reconnects on the next poll. Keep both the formatted
                // error chain and Debug representation: the latter includes
                // the concrete ConnectionError variant (e.g. timeout vs TLS/I/O).
                log::warn!("Platform MQTT disconnected: {err:#}");
                log::debug!("Platform MQTT connection error details: {err:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // This client has its own EventLoop, separate from the Home Assistant
        // MQTT client. Log its packet flow so we can tell whether CONNECT gets
        // a CONNACK, a broker rejection, or no response before the timeout.
        log::debug!("Platform MQTT event: {event:?}");

        match event {
            Event::Incoming(MqttPacket::ConnAck(_)) => {
                log::info!("Platform MQTT connected");
                // The published docs say the topic is `GA/<api-key>`, but the
                // JS example on the same page subscribes to just `<api-key>`.
                // Subscribe to both so we receive events regardless of which
                // the broker actually publishes on.
                let prefixed = format!("GA/{api_key}");
                client
                    .subscribe(&prefixed, QoS::AtLeastOnce)
                    .await
                    .with_context(|| format!("subscribe to platform topic {prefixed}"))?;
                client
                    .subscribe(&api_key, QoS::AtLeastOnce)
                    .await
                    .context("subscribe to platform topic (api-key only)")?;
            }
            Event::Incoming(MqttPacket::Publish(msg)) => {
                let payload = String::from_utf8_lossy(&msg.payload);
                log::trace!("platform-mqtt {} -> {payload}", msg.topic);

                match from_json::<EventMessage, _>(&msg.payload) {
                    Ok(event) => {
                        for cap in &event.capabilities {
                            for st in &cap.state {
                                log::debug!(
                                    "Platform event: sku={} device={} instance={} {}={}{}",
                                    event.sku,
                                    event.device,
                                    cap.instance,
                                    st.name,
                                    st.value,
                                    st.message
                                        .as_deref()
                                        .map(|m| format!(" ({m})"))
                                        .unwrap_or_default(),
                                );
                            }
                        }
                        // Notify even when capabilities is empty so subscribers
                        // get a fresh state poll opportunity.
                        state.notify_of_state_change(&event.device).await?;
                    }
                    Err(err) => {
                        log::error!("Decoding platform MQTT event: {err:#} {payload}");
                    }
                }
            }
            _ => {}
        }
    }
}
