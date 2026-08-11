#![cfg(feature = "migration-fault-injection")]

use std::collections::BTreeSet;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::anyhow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFaultMode {
    NotForwarded,
    AppliedAckLost,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitProxyTelemetry {
    pub forwarded_client_bytes_after_arm: u64,
    pub dropped_client_bytes_after_arm: u64,
    pub forwarded_server_bytes_after_arm: u64,
    pub dropped_server_bytes_after_arm: u64,
}

#[derive(Debug)]
struct ProxyState {
    mode: CommitFaultMode,
    active_connections: BTreeSet<u64>,
    armed_connection: Option<u64>,
    cut: bool,
    telemetry: CommitProxyTelemetry,
    error: Option<String>,
}

pub struct PostgresCommitProxy {
    data_port: u16,
    control_port: u16,
    state: Arc<Mutex<ProxyState>>,
    shutdown: Arc<AtomicBool>,
}

impl PostgresCommitProxy {
    pub fn start(upstream: SocketAddr, mode: CommitFaultMode) -> anyhow::Result<Self> {
        let data = TcpListener::bind(("127.0.0.1", 0))?;
        let control = TcpListener::bind(("127.0.0.1", 0))?;
        data.set_nonblocking(true)?;
        control.set_nonblocking(true)?;
        let data_port = data.local_addr()?.port();
        let control_port = control.local_addr()?.port();
        let state = Arc::new(Mutex::new(ProxyState {
            mode,
            active_connections: BTreeSet::new(),
            armed_connection: None,
            cut: false,
            telemetry: CommitProxyTelemetry::default(),
            error: None,
        }));
        let shutdown = Arc::new(AtomicBool::new(false));
        spawn_data_acceptor(data, upstream, state.clone(), shutdown.clone());
        spawn_control_acceptor(control, state.clone(), shutdown.clone());
        Ok(Self {
            data_port,
            control_port,
            state,
            shutdown,
        })
    }

    pub fn data_port(&self) -> u16 {
        self.data_port
    }

    pub fn control_port(&self) -> u16 {
        self.control_port
    }

    pub fn cut(&self) -> anyhow::Result<()> {
        let mut control = TcpStream::connect(("127.0.0.1", self.control_port))?;
        control.set_read_timeout(Some(Duration::from_secs(10)))?;
        control.set_write_timeout(Some(Duration::from_secs(10)))?;
        control.write_all(b"CUT\n")?;
        let mut response = [0_u8; 4];
        control.read_exact(&mut response)?;
        if &response != b"CUT\n" {
            return Err(anyhow!("commit proxy rejected CUT"));
        }
        Ok(())
    }

    pub fn wait_for<F>(&self, description: &str, predicate: F) -> anyhow::Result<()>
    where
        F: Fn(&CommitProxyTelemetry) -> bool,
    {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let state = self
                .state
                .lock()
                .map_err(|_| anyhow!("proxy state poisoned"))?;
            if let Some(error) = &state.error {
                return Err(anyhow!("commit proxy failed: {error}"));
            }
            if predicate(&state.telemetry) {
                return Ok(());
            }
            drop(state);
            if Instant::now() >= deadline {
                return Err(anyhow!("timed out waiting for {description}"));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn telemetry(&self) -> anyhow::Result<CommitProxyTelemetry> {
        let state = self
            .state
            .lock()
            .map_err(|_| anyhow!("proxy state poisoned"))?;
        if let Some(error) = &state.error {
            return Err(anyhow!("commit proxy failed: {error}"));
        }
        Ok(state.telemetry.clone())
    }
}

impl Drop for PostgresCommitProxy {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

fn spawn_data_acceptor(
    listener: TcpListener,
    upstream: SocketAddr,
    state: Arc<Mutex<ProxyState>>,
    shutdown: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        let mut next_connection_id = 1_u64;
        while !shutdown.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((downstream, _)) => {
                    let connection_id = next_connection_id;
                    next_connection_id = next_connection_id.saturating_add(1);
                    let state = state.clone();
                    thread::spawn(move || {
                        if let Err(error) =
                            proxy_connection(connection_id, downstream, upstream, state.clone())
                        {
                            record_error(&state, error);
                        }
                    });
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => {
                    record_error(&state, anyhow!(error).context("data accept failed"));
                    break;
                }
            }
        }
    });
}

fn spawn_control_acceptor(
    listener: TcpListener,
    state: Arc<Mutex<ProxyState>>,
    shutdown: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        while !shutdown.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    if let Err(error) = handle_control(&mut stream, &state) {
                        record_error(&state, error);
                    }
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => {
                    record_error(&state, anyhow!(error).context("control accept failed"));
                    break;
                }
            }
        }
    });
}

fn handle_control(stream: &mut TcpStream, state: &Arc<Mutex<ProxyState>>) -> anyhow::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    let mut command = [0_u8; 4];
    stream.read_exact(&mut command)?;
    match &command {
        b"ARM\n" => {
            let mut state = state.lock().map_err(|_| anyhow!("proxy state poisoned"))?;
            if state.armed_connection.is_some() || state.active_connections.len() != 1 {
                stream.write_all(b"ERROR\n")?;
                return Err(anyhow!(
                    "ARM requires exactly one active, unarmed data connection"
                ));
            }
            state.armed_connection = state.active_connections.iter().next().copied();
            stream.write_all(b"ARMED\n")?;
        }
        b"CUT\n" => {
            let mut state = state.lock().map_err(|_| anyhow!("proxy state poisoned"))?;
            if state.mode != CommitFaultMode::AppliedAckLost || state.armed_connection.is_none() {
                stream.write_all(b"ERR\n")?;
                return Err(anyhow!("CUT requires an armed acknowledgement-loss fault"));
            }
            state.cut = true;
            stream.write_all(b"CUT\n")?;
        }
        _ => return Err(anyhow!("unknown commit proxy control command")),
    }
    Ok(())
}

fn proxy_connection(
    connection_id: u64,
    downstream: TcpStream,
    upstream: SocketAddr,
    state: Arc<Mutex<ProxyState>>,
) -> anyhow::Result<()> {
    let upstream = TcpStream::connect_timeout(&upstream, Duration::from_secs(10))?;
    for stream in [&downstream, &upstream] {
        stream.set_read_timeout(Some(Duration::from_millis(100)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
    }
    state
        .lock()
        .map_err(|_| anyhow!("proxy state poisoned"))?
        .active_connections
        .insert(connection_id);
    let client_reader = downstream.try_clone()?;
    let client_writer = downstream.try_clone()?;
    let server_reader = upstream.try_clone()?;
    let server_writer = upstream.try_clone()?;
    let client_state = state.clone();
    let client = thread::spawn(move || {
        forward_client_to_server(connection_id, client_reader, server_writer, client_state)
    });
    let server_state = state.clone();
    let server = thread::spawn(move || {
        forward_server_to_client(connection_id, server_reader, client_writer, server_state)
    });
    client
        .join()
        .map_err(|_| anyhow!("client proxy thread panicked"))??;
    server
        .join()
        .map_err(|_| anyhow!("server proxy thread panicked"))??;
    state
        .lock()
        .map_err(|_| anyhow!("proxy state poisoned"))?
        .active_connections
        .remove(&connection_id);
    Ok(())
}

fn forward_client_to_server(
    connection_id: u64,
    mut source: TcpStream,
    mut destination: TcpStream,
    state: Arc<Mutex<ProxyState>>,
) -> anyhow::Result<()> {
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        if should_cut(connection_id, &state)? {
            shutdown_pair(&source, &destination);
            return Ok(());
        }
        match source.read(&mut buffer) {
            Ok(0) => {
                shutdown_pair(&source, &destination);
                return Ok(());
            }
            Ok(length) => {
                let mut state = state.lock().map_err(|_| anyhow!("proxy state poisoned"))?;
                let armed = state.armed_connection == Some(connection_id);
                if armed && state.mode == CommitFaultMode::NotForwarded {
                    state.telemetry.dropped_client_bytes_after_arm += length as u64;
                    drop(state);
                    shutdown_pair(&source, &destination);
                    return Ok(());
                }
                if armed {
                    state.telemetry.forwarded_client_bytes_after_arm += length as u64;
                }
                drop(state);
                destination.write_all(&buffer[..length])?;
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

fn forward_server_to_client(
    connection_id: u64,
    mut source: TcpStream,
    mut destination: TcpStream,
    state: Arc<Mutex<ProxyState>>,
) -> anyhow::Result<()> {
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        if should_cut(connection_id, &state)? {
            shutdown_pair(&source, &destination);
            return Ok(());
        }
        match source.read(&mut buffer) {
            Ok(0) => {
                shutdown_pair(&source, &destination);
                return Ok(());
            }
            Ok(length) => {
                let mut state = state.lock().map_err(|_| anyhow!("proxy state poisoned"))?;
                let armed = state.armed_connection == Some(connection_id);
                if armed && state.mode == CommitFaultMode::AppliedAckLost {
                    state.telemetry.dropped_server_bytes_after_arm += length as u64;
                    continue;
                }
                if armed {
                    state.telemetry.forwarded_server_bytes_after_arm += length as u64;
                }
                drop(state);
                destination.write_all(&buffer[..length])?;
            }
            Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

fn should_cut(connection_id: u64, state: &Arc<Mutex<ProxyState>>) -> anyhow::Result<bool> {
    let state = state.lock().map_err(|_| anyhow!("proxy state poisoned"))?;
    Ok(state.armed_connection == Some(connection_id) && state.cut)
}

fn shutdown_pair(first: &TcpStream, second: &TcpStream) {
    let _ = first.shutdown(Shutdown::Both);
    let _ = second.shutdown(Shutdown::Both);
}

fn record_error(state: &Arc<Mutex<ProxyState>>, error: anyhow::Error) {
    if let Ok(mut state) = state.lock() {
        state.error.get_or_insert_with(|| format!("{error:#}"));
    }
}
