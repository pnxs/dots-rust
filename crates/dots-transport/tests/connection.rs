//! Connection state-machine tests.
//!
//! Each test pairs a `Connection<DuplexStream>` (the client under test)
//! with a "fake server" task on the other end of a `tokio::io::duplex`
//! that drives the handshake from the broker side: receives Connect
//! after sending Hello, then sends ConnectResponse.

use std::sync::Arc;

use dots_core::{StructValue, dots, encode_to_vec};
use dots_derive::DotsStruct;
use dots_model::{
    DotsConnectionState, DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgHello,
    Registry, Transmission, encode_transmission, registry_with_internal_types,
};
use dots_transport::{Connection, ConnectionBuilder, ConnectionError, TransmissionCodec};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncWriteExt, DuplexStream};
use tokio_util::codec::Framed;

fn registry() -> Arc<Registry> {
    Arc::new(registry_with_internal_types())
}

/// Convert a typed `T` into a `Transmission` (DynamicStruct payload)
/// using the registry — only used by the fake server side, where we
/// don't have a `Connection::send_typed`-equivalent to lean on.
fn dynamic_for(reg: &Registry, type_name: &str, payload: &dyn StructValue) -> Transmission {
    let header = dots!(DotsHeader {
        type_name: type_name,
    });
    let descriptor = match reg.lookup(type_name).expect("type registered") {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!("expected struct entry for {type_name}"),
    };
    let bytes = encode_to_vec(payload);
    let payload = dots_core::DynamicStruct::decode(descriptor, &bytes).unwrap();
    Transmission { header, payload }
}

/// Run a "happy path" fake server: send Hello → expect Connect →
/// send ConnectResponse with `accepted = true`.
async fn happy_server(server_stream: DuplexStream, reg: Arc<Registry>) {
    let codec = TransmissionCodec::new(reg.clone());
    let mut framed = Framed::new(server_stream, codec);

    let hello = dots!(DotsMsgHello {
        server_name: "test-dotsd",
        auth_challenge: 0x4242_u64,
        authentication_required: false,
    });
    framed
        .send(dynamic_for(&reg, "DotsMsgHello", &hello))
        .await
        .unwrap();

    let connect_txn = framed.next().await.unwrap().unwrap();
    assert_eq!(connect_txn.header.type_name.as_deref(), Some("DotsMsgConnect"));

    let response = dots!(DotsMsgConnectResponse {
        server_name: "test-dotsd",
        client_id: 101_u32,
        accepted: true,
        preload: false,
        preload_finished: true,
    });
    framed
        .send(dynamic_for(&reg, "DotsMsgConnectResponse", &response))
        .await
        .unwrap();
}

#[tokio::test]
async fn establish_completes_handshake_and_records_metadata() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server = tokio::spawn(happy_server(server_io, reg.clone()));

    let conn = Connection::establish(client_io, "demo-client", reg).await.unwrap();

    assert_eq!(conn.state(), DotsConnectionState::Connected);
    assert_eq!(conn.server_name(), Some("test-dotsd"));
    assert_eq!(conn.client_id(), Some(101));

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn establish_passes_client_name_to_server() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);

        let hello = dots!(DotsMsgHello {
            server_name: "greeter",
            auth_challenge: 1_u64,
            authentication_required: false,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();

        let connect_txn = framed.next().await.unwrap().unwrap();
        // Re-decode the dynamic Connect payload to read the client name.
        let bytes = connect_txn.payload.encode();
        let connect: DotsMsgConnect = dots_core::decode_typed_from_slice(&bytes).unwrap();
        assert_eq!(connect.client_name.as_deref(), Some("named-client"));

        let response = dots!(DotsMsgConnectResponse {
            client_id: 7_u32,
            accepted: true,
        });
        framed
            .send(dynamic_for(
                &server_reg,
                "DotsMsgConnectResponse",
                &response,
            ))
            .await
            .unwrap();
    });

    let conn = Connection::establish(client_io, "named-client", reg).await.unwrap();
    assert_eq!(conn.client_id(), Some(7));
    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn establish_rejects_when_server_demands_auth() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        let hello = dots!(DotsMsgHello {
            server_name: "auth-required",
            auth_challenge: 123_u64,
            authentication_required: true,
        });
        let _ = framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await;
    });

    match Connection::establish(client_io, "client", reg).await {
        Err(ConnectionError::AuthenticationNotSupported) => {}
        other => panic!("expected AuthenticationNotSupported, got {other:?}"),
    }
    server.await.unwrap();
}

#[tokio::test]
async fn establish_with_auth_secret_completes_handshake() {
    use sha2::{Digest, Sha256};

    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();
    let nonce: u64 = 0x1122_3344_5566_7788;
    let secret = "shared-secret";
    let client_name = "auth-client";

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);

        let hello = dots!(DotsMsgHello {
            server_name: "auth-server",
            auth_challenge: nonce,
            authentication_required: true,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();

        // Receive the client's Connect, recompute the expected digest,
        // and assert the auth_challenge_response matches.
        let connect_txn = framed.next().await.unwrap().unwrap();
        assert_eq!(
            connect_txn.header.type_name.as_deref(),
            Some("DotsMsgConnect")
        );
        let bytes = connect_txn.payload.encode();
        let connect: DotsMsgConnect = dots_core::decode_typed_from_slice(&bytes).unwrap();
        let cnonce = connect.cnonce.clone().expect("cnonce present");
        let response = connect.auth_challenge_response.clone().expect("response present");

        // Re-derive the expected digest server-side.
        let mut a1 = Sha256::new();
        a1.update(client_name.as_bytes());
        a1.update(b"::");
        a1.update(secret.as_bytes());
        let a1 = a1.finalize();
        let mut h = Sha256::new();
        h.update(a1);
        h.update(b":");
        h.update(nonce.to_le_bytes());
        h.update(b":");
        h.update(cnonce.as_bytes());
        let bytes = h.finalize();
        let expected: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
        assert_eq!(response, expected);

        let resp = dots!(DotsMsgConnectResponse {
            server_name: "auth-server",
            client_id: 13_u32,
            accepted: true,
            preload: false,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgConnectResponse", &resp))
            .await
            .unwrap();
    });

    let conn = ConnectionBuilder::new(client_io, client_name, reg)
        .preload(false)
        .with_auth(secret)
        .connect()
        .await
        .unwrap();
    assert_eq!(conn.client_id(), Some(13));
    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn establish_rejects_when_server_says_not_accepted() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);

        let hello = dots!(DotsMsgHello {
            server_name: "strict",
            auth_challenge: 0_u64,
            authentication_required: false,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        let _ = framed.next().await.unwrap().unwrap();
        let response = dots!(DotsMsgConnectResponse {
            server_name: "strict",
            accepted: false,
        });
        framed
            .send(dynamic_for(
                &server_reg,
                "DotsMsgConnectResponse",
                &response,
            ))
            .await
            .unwrap();
    });

    match Connection::establish(client_io, "client", reg).await {
        Err(ConnectionError::ConnectionRejected { server_name }) => {
            assert_eq!(server_name.as_deref(), Some("strict"));
        }
        other => panic!("expected ConnectionRejected, got {other:?}"),
    }
    server.await.unwrap();
}

#[tokio::test]
async fn establish_errors_when_server_sends_unexpected_first_message() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        // Send a ConnectResponse before Hello — out of order.
        let response = dots!(DotsMsgConnectResponse {
            client_id: 1_u32,
            accepted: true,
        });
        let _ = framed
            .send(dynamic_for(
                &server_reg,
                "DotsMsgConnectResponse",
                &response,
            ))
            .await;
    });

    match Connection::establish(client_io, "client", reg).await {
        Err(ConnectionError::UnexpectedMessage { expected, got }) => {
            assert_eq!(expected, "DotsMsgHello");
            assert_eq!(got, "DotsMsgConnectResponse");
        }
        other => panic!("expected UnexpectedMessage, got {other:?}"),
    }
    server.await.unwrap();
}

#[tokio::test]
async fn establish_errors_when_server_closes_before_hello() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg = registry();

    let server = tokio::spawn(async move {
        // Server hangs up without sending anything.
        let mut s = server_io;
        s.shutdown().await.unwrap();
    });

    match Connection::establish(client_io, "client", reg).await {
        Err(ConnectionError::ConnectionClosed) => {}
        other => panic!("expected ConnectionClosed, got {other:?}"),
    }
    server.await.unwrap();
}

// ----- Post-handshake usage -----

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Demo")]
struct Demo {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    note: Option<String>,
}

#[tokio::test]
async fn send_typed_after_handshake_reaches_server() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg_owned = registry_with_internal_types();
    reg_owned.register_struct_static(Demo::DESCRIPTOR);
    let reg = Arc::new(reg_owned);

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);

        // Standard handshake.
        let hello = dots!(DotsMsgHello {
            server_name: "s",
            auth_challenge: 0_u64,
            authentication_required: false,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        let _connect = framed.next().await.unwrap().unwrap();
        let response = dots!(DotsMsgConnectResponse {
            client_id: 1_u32,
            accepted: true,
        });
        framed
            .send(dynamic_for(
                &server_reg,
                "DotsMsgConnectResponse",
                &response,
            ))
            .await
            .unwrap();

        // Now expect the client's typed Demo publish.
        let demo_txn = framed.next().await.unwrap().unwrap();
        assert_eq!(demo_txn.header.type_name.as_deref(), Some("Demo"));
        let bytes = demo_txn.payload.encode();
        let decoded: Demo = dots_core::decode_typed_from_slice(&bytes).unwrap();
        assert_eq!(decoded.id, Some(42));
        assert_eq!(decoded.note.as_deref(), Some("hello dotsd"));
    });

    let mut conn = Connection::establish(client_io, "publisher", reg).await.unwrap();
    let demo = dots!(Demo {
        id: 42_u32,
        note: "hello dotsd",
    });
    conn.send_typed(&demo).await.unwrap();

    drop(conn);
    server.await.unwrap();
}

#[tokio::test]
async fn next_after_handshake_yields_server_traffic() {
    let (client_io, server_io) = tokio::io::duplex(4096);
    let reg_owned = registry_with_internal_types();
    reg_owned.register_struct_static(Demo::DESCRIPTOR);
    let reg = Arc::new(reg_owned);

    let server_reg = reg.clone();
    let server = tokio::spawn(async move {
        let codec = TransmissionCodec::new(server_reg.clone());
        let mut framed = Framed::new(server_io, codec);
        let hello = dots!(DotsMsgHello {
            server_name: "s",
            auth_challenge: 0_u64,
            authentication_required: false,
        });
        framed
            .send(dynamic_for(&server_reg, "DotsMsgHello", &hello))
            .await
            .unwrap();
        let _connect = framed.next().await.unwrap().unwrap();
        let response = dots!(DotsMsgConnectResponse {
            client_id: 2_u32,
            accepted: true,
        });
        framed
            .send(dynamic_for(
                &server_reg,
                "DotsMsgConnectResponse",
                &response,
            ))
            .await
            .unwrap();

        // Push a Demo at the client.
        let demo = dots!(Demo {
            id: 7_u32,
            note: "from server",
        });
        let header = dots!(DotsHeader {
            type_name: "Demo",
            sender: 99_u32,
        });
        let bytes = encode_transmission(&header, &demo);
        let stream = framed.get_mut();
        stream.write_all(&bytes).await.unwrap();
    });

    let mut conn = Connection::establish(client_io, "subscriber", reg).await.unwrap();
    let txn = conn.next().await.expect("server traffic arrives").unwrap();
    assert_eq!(txn.header.type_name.as_deref(), Some("Demo"));
    assert_eq!(txn.header.sender, Some(99));

    drop(conn);
    server.await.unwrap();
}
