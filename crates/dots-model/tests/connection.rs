//! Roundtrip and wire-shape tests for the connection-layer types.

use std::sync::Arc;

use dots_core::{DynamicStruct, Timepoint, decode_typed_from_slice, encode_to_vec};
use dots_model::{
    DotsConnectionState, DotsHeader, DotsMsgConnect, DotsMsgConnectResponse, DotsMsgError,
    DotsMsgHello, Registry, StructDescriptorData,
};

// ----- DotsHeader -----

#[test]
fn dots_header_roundtrip() {
    let original = DotsHeader {
        type_name: Some("MyType".into()),
        sent_time: Some(Timepoint(1_700_000_000.0)),
        server_sent_time: Some(Timepoint(1_700_000_000.5)),
        attributes: Some(0b0000_0000_0000_1011),
        sender: Some(42),
        from_cache: Some(7),
        remove_obj: Some(false),
        is_from_myself: Some(false),
    };
    let bytes = encode_to_vec(&original);
    let decoded: DotsHeader = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(original, decoded);
}

#[test]
fn dots_header_partial_object_omits_unset_fields() {
    let h = DotsHeader {
        type_name: Some("Foo".into()),
        sender: Some(1),
        ..Default::default()
    };
    let bytes = encode_to_vec(&h);
    // CBOR map of 2 pairs is 0xa2.
    assert_eq!(bytes[0], 0xa2);
}

// ----- Handshake sequence: Hello → Connect → ConnectResponse -----

#[test]
fn hello_roundtrip() {
    let hello = DotsMsgHello {
        server_name: Some("dotsd".into()),
        auth_challenge: Some(0xdeadbeef_cafef00d_u64),
        authentication_required: Some(true),
    };
    let bytes = encode_to_vec(&hello);
    let decoded: DotsMsgHello = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(hello, decoded);
}

#[test]
fn connect_roundtrip_first_phase() {
    // Phase 1: client introduces itself, requests preload.
    let connect = DotsMsgConnect {
        client_name: Some("client-A".into()),
        preload_cache: Some(true),
        preload_client_finished: None,
        auth_challenge_response: Some("hex-digest".into()),
        cnonce: Some("client-nonce".into()),
    };
    let bytes = encode_to_vec(&connect);
    let decoded: DotsMsgConnect = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(connect, decoded);
    // Only the four set fields go on the wire.
    assert_eq!(decoded.preload_client_finished, None);
}

#[test]
fn connect_roundtrip_second_phase() {
    // Phase 2: client signals subscriptions complete.
    let connect = DotsMsgConnect {
        preload_client_finished: Some(true),
        ..Default::default()
    };
    let bytes = encode_to_vec(&connect);
    // CBOR map of 1 pair: 0xa1, key=3, value=true (0xf5).
    assert_eq!(bytes, [0xa1, 0x03, 0xf5]);
}

#[test]
fn connect_response_roundtrip() {
    let resp = DotsMsgConnectResponse {
        server_name: Some("dotsd".into()),
        client_id: Some(101),
        accepted: Some(true),
        preload: Some(true),
        preload_finished: Some(false),
    };
    let bytes = encode_to_vec(&resp);
    let decoded: DotsMsgConnectResponse = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(resp, decoded);
}

#[test]
fn connect_response_non_contiguous_tags_round_trip() {
    // Tags 1, 5, 2, 3, 4 in source order — verify the encoded map
    // is keyed by tag value, not source order, and that decode pairs
    // values back to their named fields correctly.
    let resp = DotsMsgConnectResponse {
        client_id: Some(7),       // tag 5
        accepted: Some(false),    // tag 2
        ..Default::default()
    };
    let bytes = encode_to_vec(&resp);
    let decoded: DotsMsgConnectResponse = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(decoded.client_id, Some(7));
    assert_eq!(decoded.accepted, Some(false));
    assert_eq!(decoded.server_name, None);
}

// ----- DotsMsgError -----

#[test]
fn error_roundtrip_negative_code() {
    let err = DotsMsgError {
        error_code: Some(-13),
        error_text: Some("auth failed".into()),
    };
    let bytes = encode_to_vec(&err);
    let decoded: DotsMsgError = decode_typed_from_slice(&bytes).unwrap();
    assert_eq!(err, decoded);
}

// ----- DotsConnectionState -----

#[test]
fn connection_state_roundtrip_each_variant() {
    use dots_core::DotsField;
    for state in [
        DotsConnectionState::Connecting,
        DotsConnectionState::EarlySubscribe,
        DotsConnectionState::Connected,
        DotsConnectionState::Suspended,
        DotsConnectionState::Closed,
    ] {
        let mut buf: Vec<u8> = Vec::new();
        {
            let mut e = dots_core::minicbor::Encoder::new(&mut buf);
            state.dots_encode(&mut e).unwrap();
        }
        let mut d = dots_core::minicbor::Decoder::new(&buf);
        let decoded = DotsConnectionState::dots_decode(&mut d).unwrap();
        assert_eq!(decoded, state);
    }
}

// ----- DynamicStruct round-trip via the registry -----

#[test]
fn dots_header_decodes_via_wire_only_path() {
    // A wire-receiver who only knows the descriptor (no compiled type)
    // can decode a DotsHeader via the registry → DynamicStructDescriptor
    // path, the same way it would for any user-defined type.
    let reg = Registry::new();
    reg.register_struct_static(DotsHeader::DESCRIPTOR);

    let original = DotsHeader {
        type_name: Some("Sample".into()),
        sender: Some(123),
        sent_time: Some(Timepoint(2_500_000_000.0)),
        ..Default::default()
    };
    let typed_bytes = encode_to_vec(&original);

    let dyn_desc = match reg.lookup("DotsHeader").unwrap() {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let dyn_value = DynamicStruct::decode(dyn_desc, &typed_bytes).unwrap();
    assert_eq!(dyn_value.encode(), typed_bytes);
}

#[test]
fn handshake_descriptors_resolve_through_registry_from_wire() {
    // Same as above but with descriptor-data round-tripping through
    // the registry's reverse conversion — proves a peer who learns
    // the handshake structs entirely from the wire can decode them.
    let reg = Registry::new();
    for static_desc in [
        DotsMsgHello::DESCRIPTOR,
        DotsMsgConnect::DESCRIPTOR,
        DotsMsgConnectResponse::DESCRIPTOR,
        DotsMsgError::DESCRIPTOR,
        DotsHeader::DESCRIPTOR,
    ] {
        let data = StructDescriptorData::from_static(static_desc);
        let dyn_desc = reg.build_dynamic_struct(&data).unwrap();
        reg.register_struct_dynamic(Arc::new(dyn_desc));
    }

    // Encode a real Hello, decode it through the wire-only registry path.
    let hello = DotsMsgHello {
        server_name: Some("dotsd".into()),
        auth_challenge: Some(42),
        authentication_required: Some(false),
    };
    let typed_bytes = encode_to_vec(&hello);

    let dyn_desc = match reg.lookup("DotsMsgHello").unwrap() {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let dyn_value = DynamicStruct::decode(dyn_desc, &typed_bytes).unwrap();
    assert_eq!(dyn_value.encode(), typed_bytes);

    // Re-decode via typed path for full closure.
    let back: DotsMsgHello = decode_typed_from_slice(&typed_bytes).unwrap();
    assert_eq!(back, hello);
}
