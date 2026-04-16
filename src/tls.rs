//! Native TLS context and connection types backed by rustls.
//!
//! # Python API
//!
//!   # Server side
//!   ctx = RsloopTLSContext.server(cert_chain_pem, private_key_pem)
//!
//!   # Client side, no certificate verification (mirrors ssl.CERT_NONE)
//!   ctx = RsloopTLSContext.client(verify=False)
//!
//!   # Client side with an explicit CA bundle
//!   ctx = RsloopTLSContext.client(ca_certs_pem=pem_bytes)

#![cfg(unix)]

use std::io;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use rustls::client::{ClientConnectionData, UnbufferedClientConnection};
use rustls::server::{ServerConnectionData, UnbufferedServerConnection};
use rustls::unbuffered::{ConnectionState, EncodeError, EncryptError, UnbufferedStatus};
use rustls::{ClientConfig, ServerConfig, SignatureScheme};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};

const OUT_CAP: usize = 18 * 1024;

// ---------------------------------------------------------------------------
// NoCertVerifier
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct NoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

// ---------------------------------------------------------------------------
// RsloopTLSContext
// ---------------------------------------------------------------------------

pub(crate) enum TlsConfigInner {
    Client(Arc<ClientConfig>),
    Server(Arc<ServerConfig>),
}

/// Python-visible TLS context. Pass as ssl= to create_connection/create_server
/// to use the native rustls data path instead of Python's ssl module.
#[pyclass(name = "RsloopTLSContext", module = "rsloop._rsloop")]
pub struct RsloopTLSContext {
    pub(crate) inner: TlsConfigInner,
}

#[pymethods]
impl RsloopTLSContext {
    /// Build a server-side context from PEM cert chain and private key.
    #[staticmethod]
    #[pyo3(signature = (cert_chain_pem, private_key_pem))]
    fn server(
        py: Python<'_>,
        cert_chain_pem: Py<PyAny>,
        private_key_pem: Py<PyAny>,
    ) -> PyResult<Self> {
        let cert_pem = pem_to_bytes(py, &cert_chain_pem)?;
        let key_pem = pem_to_bytes(py, &private_key_pem)?;
        let certs = parse_cert_chain(&cert_pem)
            .map_err(|e| PyValueError::new_err(format!("cert_chain_pem: {e}")))?;
        let key = parse_private_key(&key_pem)
            .map_err(|e| PyValueError::new_err(format!("private_key_pem: {e}")))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| PyValueError::new_err(format!("TLS protocol versions: {e}")))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| PyValueError::new_err(format!("TLS server config: {e}")))?;
        Ok(Self {
            inner: TlsConfigInner::Server(Arc::new(config)),
        })
    }

    /// Build a client-side context.
    /// verify=False skips cert verification. ca_certs_pem provides a CA bundle.
    #[staticmethod]
    #[pyo3(signature = (verify=true, ca_certs_pem=None))]
    fn client(py: Python<'_>, verify: bool, ca_certs_pem: Option<Py<PyAny>>) -> PyResult<Self> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = if !verify {
            ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| PyValueError::new_err(format!("TLS versions: {e}")))?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
                .with_no_client_auth()
        } else if let Some(ca_pem) = ca_certs_pem {
            let pem = pem_to_bytes(py, &ca_pem)?;
            let mut root_store = rustls::RootCertStore::empty();
            for cert in parse_cert_chain(&pem)
                .map_err(|e| PyValueError::new_err(format!("ca_certs_pem: {e}")))?
            {
                root_store
                    .add(cert)
                    .map_err(|e| PyValueError::new_err(format!("CA cert: {e}")))?;
            }
            ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| PyValueError::new_err(format!("TLS versions: {e}")))?
                .with_root_certificates(root_store)
                .with_no_client_auth()
        } else {
            ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| PyValueError::new_err(format!("TLS versions: {e}")))?
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth()
        };
        Ok(Self {
            inner: TlsConfigInner::Client(Arc::new(config)),
        })
    }

    #[getter]
    fn is_server(&self) -> bool {
        matches!(self.inner, TlsConfigInner::Server(_))
    }
}

// ---------------------------------------------------------------------------
// RsloopTLSConn – per-connection live state (not exported to Python)
// ---------------------------------------------------------------------------

pub(crate) enum RsloopTLSConn {
    Client(TlsConnInner<UnbufferedClientConnection>),
    Server(TlsConnInner<UnbufferedServerConnection>),
}

pub(crate) struct TlsConnInner<C> {
    pub conn: C,
    pub incoming: Vec<u8>,
    pub outgoing: Vec<u8>,
    pub handshake_done: bool,
    pub peer_closed: bool,
}

impl RsloopTLSConn {
    pub(crate) fn new_client(
        config: Arc<ClientConfig>,
        server_name: &str,
    ) -> Result<Self, rustls::Error> {
        let sni = ServerName::try_from(server_name)
            .map(|n| n.to_owned())
            .map_err(|_| rustls::Error::General(format!("invalid SNI: {server_name}")))?;
        let conn = UnbufferedClientConnection::new(config, sni)?;
        Ok(Self::Client(TlsConnInner {
            conn,
            incoming: Vec::new(),
            outgoing: Vec::with_capacity(OUT_CAP),
            handshake_done: false,
            peer_closed: false,
        }))
    }

    pub(crate) fn new_server(config: Arc<ServerConfig>) -> Result<Self, rustls::Error> {
        let conn = UnbufferedServerConnection::new(config)?;
        Ok(Self::Server(TlsConnInner {
            conn,
            incoming: Vec::new(),
            outgoing: Vec::with_capacity(OUT_CAP),
            handshake_done: false,
            peer_closed: false,
        }))
    }

    pub(crate) fn incoming_mut(&mut self) -> &mut Vec<u8> {
        match self {
            Self::Client(c) => &mut c.incoming,
            Self::Server(s) => &mut s.incoming,
        }
    }

    pub(crate) fn outgoing(&self) -> &[u8] {
        match self {
            Self::Client(c) => &c.outgoing,
            Self::Server(s) => &s.outgoing,
        }
    }

    pub(crate) fn outgoing_mut(&mut self) -> &mut Vec<u8> {
        match self {
            Self::Client(c) => &mut c.outgoing,
            Self::Server(s) => &mut s.outgoing,
        }
    }

    pub(crate) fn handshake_done(&self) -> bool {
        match self {
            Self::Client(c) => c.handshake_done,
            Self::Server(s) => s.handshake_done,
        }
    }

    pub(crate) fn peer_closed(&self) -> bool {
        match self {
            Self::Client(c) => c.peer_closed,
            Self::Server(s) => s.peer_closed,
        }
    }

    pub(crate) fn process_incoming(
        &mut self,
        plaintext_out: &mut Vec<u8>,
    ) -> Result<(usize, bool), io::Error> {
        match self {
            Self::Client(inner) => inner.process_incoming(plaintext_out),
            Self::Server(inner) => inner.process_incoming(plaintext_out),
        }
    }

    pub(crate) fn encrypt_append(&mut self, plaintext: &[u8]) -> Result<(), io::Error> {
        match self {
            Self::Client(inner) => inner.encrypt_append(plaintext),
            Self::Server(inner) => inner.encrypt_append(plaintext),
        }
    }

    /// Drive the rustls state machine once even with no incoming data.
    /// This allows a client connection to generate and buffer its ClientHello
    /// without waiting for any incoming bytes.
    pub(crate) fn kick_handshake(&mut self) -> Result<bool, io::Error> {
        let mut dummy = Vec::new();
        let (_, needs_send) = self.process_incoming_raw(&mut dummy)?;
        Ok(needs_send)
    }

    fn process_incoming_raw(
        &mut self,
        plaintext_out: &mut Vec<u8>,
    ) -> Result<(usize, bool), io::Error> {
        match self {
            Self::Client(inner) => inner.process_incoming_raw(plaintext_out),
            Self::Server(inner) => inner.process_incoming_raw(plaintext_out),
        }
    }
}

// ---------------------------------------------------------------------------
// Generic drive loop
// ---------------------------------------------------------------------------

trait ProcessTlsRecords {
    type Data;
    fn process_records<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data>;
}

impl ProcessTlsRecords for UnbufferedClientConnection {
    type Data = ClientConnectionData;
    fn process_records<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(incoming)
    }
}

impl ProcessTlsRecords for UnbufferedServerConnection {
    type Data = ServerConnectionData;
    fn process_records<'c, 'i>(
        &'c mut self,
        incoming: &'i mut [u8],
    ) -> UnbufferedStatus<'c, 'i, Self::Data> {
        self.process_tls_records(incoming)
    }
}

impl<C: ProcessTlsRecords> TlsConnInner<C> {
    fn process_incoming(
        &mut self,
        plaintext_out: &mut Vec<u8>,
    ) -> Result<(usize, bool), io::Error> {
        let mut plaintext_written = 0usize;
        let mut needs_send = !self.outgoing.is_empty();

        // Debug: show first bytes if processing fails
        #[cfg(debug_assertions)]
        let _incoming_preview: Vec<u8> = self.incoming.iter().take(8).cloned().collect();
        
        // Keep driving the state machine until we are truly blocked or done.
        // rustls can emit multiple sequential EncodeTlsData/TransmitTlsData
        // states (e.g. ServerHello, Certificate, CertVerify, Finished) before
        // blocking.  We must loop until we hit BlockedHandshake, WriteTraffic,
        // ReadTraffic, or detect genuine no-progress.
        //
        // The `consecutive_no_progress` counter prevents infinite loops:
        // we allow a few passes with (discard=0, incoming empty) because
        // EncodeTlsData can produce handshake records without consuming
        // any incoming bytes.  Once we see BlockedHandshake or two consecutive
        // no-progress passes with no encoding, we stop.
        let mut consecutive_no_progress = 0usize;
        loop {
            let status = self.conn.process_records(&mut self.incoming);
            let discard = status.discard;

            let made_progress = discard > 0; // consumed some incoming bytes

            match status.state {
                Ok(ConnectionState::EncodeTlsData(mut enc)) => {
                    encode_into_outgoing(&mut enc, &mut self.outgoing)?;
                    needs_send = true;
                    consecutive_no_progress = 0; // encoding is progress
                }
                Ok(ConnectionState::TransmitTlsData(tls)) => {
                    needs_send = true;
                    tls.done();
                    consecutive_no_progress = 0;
                }
                Ok(ConnectionState::BlockedHandshake) => {
                    // Genuinely need more incoming data.
                    if discard > 0 { self.incoming.drain(..discard); }
                    break;
                }
                Ok(ConnectionState::WriteTraffic(_)) => {
                    self.handshake_done = true;
                    if discard > 0 { self.incoming.drain(..discard); }
                    break;
                }
                Ok(ConnectionState::ReadTraffic(mut rt)) => {
                    self.handshake_done = true;
                    loop {
                        match rt.next_record() {
                            Some(Ok(app)) => {
                                plaintext_out.extend_from_slice(app.payload);
                                plaintext_written += app.payload.len();
                            }
                            Some(Err(e)) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("TLS read error: {e}"),
                                ));
                            }
                            None => break,
                        }
                    }
                    if discard > 0 { self.incoming.drain(..discard); }
                    if !made_progress {
                        consecutive_no_progress += 1;
                        if consecutive_no_progress >= 2 { break; }
                    } else {
                        consecutive_no_progress = 0;
                    }
                    continue; // check for more app data records
                }
                Ok(ConnectionState::PeerClosed) | Ok(ConnectionState::Closed) => {
                    self.peer_closed = true;
                    if discard > 0 { self.incoming.drain(..discard); }
                    break;
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("TLS error: {e}"),
                    ));
                }
                Ok(_) => {
                    if discard > 0 { self.incoming.drain(..discard); }
                    consecutive_no_progress += 1;
                    if consecutive_no_progress >= 2 { break; }
                    continue;
                }
            }

            if discard > 0 { self.incoming.drain(..discard); }

            if !made_progress {
                consecutive_no_progress += 1;
                // Allow more passes for EncodeTlsData/TransmitTlsData but
                // cap to avoid spinning indefinitely.
                if consecutive_no_progress >= 32 { break; }
            } else {
                consecutive_no_progress = 0;
            }
        }

        if !self.outgoing.is_empty() {
            needs_send = true;
        }
        Ok((plaintext_written, needs_send))
    }

    fn encrypt_append(&mut self, plaintext: &[u8]) -> Result<(), io::Error> {
        if plaintext.is_empty() {
            return Ok(());
        }

        // We must handle the state (including any encrypt calls) before draining
        // incoming, because `status` holds a borrow on `self.incoming`.
        enum EncryptOutcome {
            Done,
            HandshakeRecord,
            NotReady,
            Error(io::Error),
        }

        let outcome = {
            let status = self.conn.process_records(&mut self.incoming);
            let discard = status.discard;

            let result = match status.state {
                Ok(ConnectionState::WriteTraffic(mut wt)) => {
                    let start = self.outgoing.len();
                    let needed = plaintext.len() + 300;
                    self.outgoing.resize(start + needed, 0);
                    match wt.encrypt(plaintext, &mut self.outgoing[start..]) {
                        Ok(n) => {
                            self.outgoing.truncate(start + n);
                            EncryptOutcome::Done
                        }
                        Err(EncryptError::InsufficientSize(e)) => {
                            self.outgoing.truncate(start);
                            self.outgoing.resize(start + e.required_size, 0);
                            match wt.encrypt(plaintext, &mut self.outgoing[start..]) {
                                Ok(n) => {
                                    self.outgoing.truncate(start + n);
                                    EncryptOutcome::Done
                                }
                                Err(e2) => EncryptOutcome::Error(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("TLS encrypt: {e2:?}"),
                                )),
                            }
                        }
                        Err(e) => EncryptOutcome::Error(io::Error::new(
                            io::ErrorKind::Other,
                            format!("TLS encrypt: {e:?}"),
                        )),
                    }
                }
                Ok(ConnectionState::EncodeTlsData(mut enc)) => {
                    match encode_into_outgoing(&mut enc, &mut self.outgoing) {
                        Ok(()) => EncryptOutcome::HandshakeRecord,
                        Err(e) => EncryptOutcome::Error(e),
                    }
                }
                Ok(_) => EncryptOutcome::NotReady,
                Err(e) => EncryptOutcome::Error(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("TLS error: {e}"),
                )),
            };
            // status is dropped here, releasing the borrow on self.incoming.
            (discard, result)
        };

        let (discard, result) = outcome;
        if discard > 0 {
            self.incoming.drain(..discard);
        }

        // If we got a post-handshake record, keep looping to find WriteTraffic.
        match result {
            EncryptOutcome::Done => return Ok(()),
            EncryptOutcome::Error(e) => return Err(e),
            EncryptOutcome::NotReady | EncryptOutcome::HandshakeRecord => {}
        }

        // One more loop: process any post-handshake records (e.g. NewSessionTicket)
        // to reach WriteTraffic.
        for _ in 0..8 {
            enum EncryptStep { Done, Retry, Blocked, Error(io::Error) }
            let outcome2 = {
                let status = self.conn.process_records(&mut self.incoming);
                let discard2 = status.discard;
                let step = match status.state {
                    Ok(ConnectionState::WriteTraffic(mut wt)) => {
                        let start = self.outgoing.len();
                        let needed = plaintext.len() + 300;
                        self.outgoing.resize(start + needed, 0);
                        match wt.encrypt(plaintext, &mut self.outgoing[start..]) {
                            Ok(n) => { self.outgoing.truncate(start + n); EncryptStep::Done }
                            Err(EncryptError::InsufficientSize(e)) => {
                                self.outgoing.truncate(start);
                                self.outgoing.resize(start + e.required_size, 0);
                                match wt.encrypt(plaintext, &mut self.outgoing[start..]) {
                                    Ok(n) => { self.outgoing.truncate(start + n); EncryptStep::Done }
                                    Err(e2) => EncryptStep::Error(io::Error::new(io::ErrorKind::Other, format!("TLS encrypt: {e2:?}")))
                                }
                            }
                            Err(e) => EncryptStep::Error(io::Error::new(io::ErrorKind::Other, format!("TLS encrypt: {e:?}")))
                        }
                    }
                    Ok(ConnectionState::EncodeTlsData(mut enc)) => {
                        match encode_into_outgoing(&mut enc, &mut self.outgoing) {
                            Ok(()) => if discard2 > 0 { EncryptStep::Retry } else { EncryptStep::Blocked },
                            Err(e) => EncryptStep::Error(e),
                        }
                    }
                    Ok(ConnectionState::TransmitTlsData(tls)) => { tls.done(); EncryptStep::Retry }
                    Ok(ConnectionState::ReadTraffic(_)) => if discard2 > 0 { EncryptStep::Retry } else { EncryptStep::Blocked },
                    Ok(ConnectionState::BlockedHandshake) | Ok(_) => EncryptStep::Blocked,
                    Err(e) => EncryptStep::Error(io::Error::new(io::ErrorKind::InvalidData, format!("TLS error: {e}"))),
                };
                (discard2, step)
            };
            let (discard2, step) = outcome2;
            if discard2 > 0 { self.incoming.drain(..discard2); }
            match step {
                EncryptStep::Done => return Ok(()),
                EncryptStep::Error(e) => return Err(e),
                EncryptStep::Blocked => break,
                EncryptStep::Retry => {} // continue loop
            }
        }

        Err(io::Error::new(io::ErrorKind::WouldBlock, "TLS not in write-traffic state"))
    }

    /// Run one step of the TLS state machine even when incoming is empty.
    /// This allows a client connection to generate its ClientHello on first call.
    fn process_incoming_raw(
        &mut self,
        plaintext_out: &mut Vec<u8>,
    ) -> Result<(usize, bool), io::Error> {
        let mut needs_send = !self.outgoing.is_empty();

        // Call process_records with whatever is in incoming (may be empty).
        // rustls will produce EncodeTlsData if it has pending handshake output.
        let status = self.conn.process_records(&mut self.incoming);
        let discard = status.discard;

        match status.state {
            Ok(ConnectionState::EncodeTlsData(mut enc)) => {
                encode_into_outgoing(&mut enc, &mut self.outgoing)?;
                needs_send = true;
            }
            Ok(ConnectionState::TransmitTlsData(tls)) => {
                needs_send = true;
                tls.done();
            }
            Ok(ConnectionState::ReadTraffic(mut rt)) => {
                self.handshake_done = true;
                loop {
                    match rt.next_record() {
                        Some(Ok(app)) => {
                            plaintext_out.extend_from_slice(app.payload);
                        }
                        Some(Err(e)) => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("TLS read error: {e}"),
                            ));
                        }
                        None => break,
                    }
                }
            }
            Ok(ConnectionState::WriteTraffic(_)) => {
                self.handshake_done = true;
            }
            Ok(ConnectionState::PeerClosed) | Ok(ConnectionState::Closed) => {
                self.peer_closed = true;
            }
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("TLS error: {e}"),
                ));
            }
            Ok(_) => {}
        }

        if discard > 0 {
            self.incoming.drain(..discard);
        }

        if !self.outgoing.is_empty() {
            needs_send = true;
        }
        Ok((0, needs_send))
    }
}

fn encode_into_outgoing<Data>(
    enc: &mut rustls::unbuffered::EncodeTlsData<'_, Data>,
    outgoing: &mut Vec<u8>,
) -> Result<(), io::Error> {
    let start = outgoing.len();
    outgoing.resize(start + OUT_CAP, 0);
    match enc.encode(&mut outgoing[start..]) {
        Ok(n) => {
            outgoing.truncate(start + n);
            Ok(())
        }
        Err(EncodeError::InsufficientSize(e)) => {
            outgoing.truncate(start);
            outgoing.resize(start + e.required_size, 0);
            match enc.encode(&mut outgoing[start..]) {
                Ok(n) => {
                    outgoing.truncate(start + n);
                    Ok(())
                }
                Err(e2) => Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("TLS encode: {e2:?}"),
                )),
            }
        }
        Err(e) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("TLS encode: {e:?}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// PEM helpers
// ---------------------------------------------------------------------------

fn pem_to_bytes(py: Python<'_>, obj: &Py<PyAny>) -> PyResult<Vec<u8>> {
    let b = obj.bind(py);
    if let Ok(bytes) = b.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    if let Ok(s) = b.extract::<&str>() {
        return Ok(s.as_bytes().to_vec());
    }
    Err(PyValueError::new_err("expected bytes or str for PEM data"))
}

fn parse_cert_chain(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, String> {
    let mut reader = io::BufReader::new(pem);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("PEM parse error: {e}"))?;
    if certs.is_empty() {
        return Err("no certificates found in PEM".into());
    }
    Ok(certs)
}

fn parse_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, String> {
    let mut reader = io::BufReader::new(pem);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| format!("PEM parse error: {e}"))?
        .ok_or_else(|| "no private key found in PEM".to_string())
}
