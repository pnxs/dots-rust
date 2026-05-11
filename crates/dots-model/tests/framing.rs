//! v2 transmission framing tests.

use std::sync::Arc;

use dots_core::encode_to_vec;
use dots_derive::DotsStruct;
use bytes::Bytes;
use dots_model::{
    DotsHeader, FramingError, MAX_BODY_SIZE, RawTransmission, Registry, SIZE_PREFIX_LEN,
    SIZE_PREFIX_MARKER, StructDescriptorData, Transmission, decode_typed_transmission,
    encode_frame_with_header, encode_transmission, encode_transmission_into,
    parse_size_prefix,
};

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Sample", cached)]
struct Sample {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    label: Option<String>,
}

fn header_for(payload_type: &str) -> DotsHeader {
    DotsHeader {
        type_name: Some(payload_type.into()),
        sender: Some(7),
        ..Default::default()
    }
}

fn populated_registry() -> Registry {
    let reg = Registry::new();
    let data = StructDescriptorData::from_static(Sample::DESCRIPTOR);
    let dyn_desc = reg.build_dynamic_struct(&data).unwrap();
    reg.register_struct_dynamic(Arc::new(dyn_desc));
    reg
}

// ----- Wire format / encoding -----

#[test]
fn wire_starts_with_marker_and_be_size() {
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(1),
        label: Some("hi".into()),
    };
    let bytes = encode_transmission(&header, &payload);

    // 0x1A marker.
    assert_eq!(bytes[0], SIZE_PREFIX_MARKER);

    // Body size matches what's after the prefix.
    let advertised = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    assert_eq!(advertised, bytes.len() - SIZE_PREFIX_LEN);
}

#[test]
fn body_is_header_then_payload_concatenated() {
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(1),
        ..Default::default()
    };
    let frame = encode_transmission(&header, &payload);
    let body = &frame[SIZE_PREFIX_LEN..];

    // Header alone should be a prefix of the body.
    let header_bytes = encode_to_vec(&header);
    assert!(
        body.starts_with(&header_bytes),
        "body must begin with the header CBOR bytes"
    );
    // And the rest is exactly the payload's CBOR.
    let payload_bytes = encode_to_vec(&payload);
    assert_eq!(&body[header_bytes.len()..], payload_bytes.as_slice());
}

// ----- parse_size_prefix -----

#[test]
fn parse_size_prefix_extracts_body_size() {
    let mut buf = vec![SIZE_PREFIX_MARKER];
    buf.extend_from_slice(&123u32.to_be_bytes());
    assert_eq!(parse_size_prefix(&buf).unwrap(), 123);
}

#[test]
fn parse_size_prefix_needs_more_data() {
    let buf = [SIZE_PREFIX_MARKER, 0, 0]; // only 3 bytes
    match parse_size_prefix(&buf) {
        Err(FramingError::NeedMoreData { have: 3, need: 5 }) => {}
        other => panic!("expected NeedMoreData, got {other:?}"),
    }
}

#[test]
fn parse_size_prefix_rejects_wrong_marker() {
    let buf = [0xFF_u8, 0, 0, 0, 1];
    match parse_size_prefix(&buf) {
        Err(FramingError::InvalidSizePrefix(0xFF)) => {}
        other => panic!("expected InvalidSizePrefix, got {other:?}"),
    }
}

#[test]
fn parse_size_prefix_rejects_oversize() {
    let mut buf = vec![SIZE_PREFIX_MARKER];
    let too_big = MAX_BODY_SIZE + 1;
    buf.extend_from_slice(&too_big.to_be_bytes());
    match parse_size_prefix(&buf) {
        Err(FramingError::BodyTooLarge { size }) if size == too_big => {}
        other => panic!("expected BodyTooLarge, got {other:?}"),
    }
}

// ----- Typed roundtrip -----

#[test]
fn typed_decode_recovers_header_and_payload() {
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(42),
        label: Some("hello".into()),
    };
    let frame = encode_transmission(&header, &payload);

    let (h, p, consumed) = decode_typed_transmission::<Sample>(&frame).unwrap();
    assert_eq!(h, header);
    assert_eq!(p, payload);
    assert_eq!(consumed, frame.len());
}

#[test]
fn typed_decode_with_partial_buffer_signals_need_more() {
    let frame = encode_transmission(&header_for("Sample"), &Sample::default());
    // Hand the decoder a truncated buffer.
    let truncated = &frame[..frame.len() - 1];
    match decode_typed_transmission::<Sample>(truncated) {
        Err(FramingError::NeedMoreData { .. }) => {}
        other => panic!("expected NeedMoreData, got {other:?}"),
    }
}

// ----- Dynamic roundtrip via Registry -----

#[test]
fn dynamic_decode_recovers_payload_via_registry() {
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(99),
        label: Some("dyn".into()),
    };
    let frame = encode_transmission(&header, &payload);

    let registry = populated_registry();
    let (txn, consumed) = Transmission::decode(&frame, &registry).unwrap();
    assert_eq!(consumed, frame.len());
    assert_eq!(txn.header, header);
    // Re-encode the dynamic transmission — bytes must match.
    let re = txn.encode();
    assert_eq!(re, frame);
}

#[test]
fn dynamic_decode_fails_on_unknown_type() {
    let header = header_for("NotRegistered");
    let payload = Sample {
        id: Some(1),
        ..Default::default()
    };
    let frame = encode_transmission(&header, &payload);

    let registry = populated_registry();
    match Transmission::decode(&frame, &registry) {
        Err(FramingError::UnknownType(name)) => assert_eq!(name, "NotRegistered"),
        other => panic!("expected UnknownType, got {other:?}"),
    }
}

#[test]
fn dynamic_decode_fails_when_header_lacks_type_name() {
    let header = DotsHeader {
        sender: Some(1),
        ..Default::default()
    };
    let frame = encode_transmission(&header, &Sample::default());

    let registry = populated_registry();
    match Transmission::decode(&frame, &registry) {
        Err(FramingError::HeaderMissingTypeName) => {}
        other => panic!("expected HeaderMissingTypeName, got {other:?}"),
    }
}

// ----- Streaming: multiple frames in one buffer -----

#[test]
fn back_to_back_frames_decode_with_correct_offsets() {
    let header = header_for("Sample");
    let p1 = Sample {
        id: Some(1),
        label: Some("first".into()),
    };
    let p2 = Sample {
        id: Some(2),
        label: Some("second".into()),
    };

    let mut stream = encode_transmission(&header, &p1);
    let frame1_len = stream.len();
    stream.extend_from_slice(&encode_transmission(&header, &p2));

    let registry = populated_registry();

    let (t1, consumed1) = Transmission::decode(&stream, &registry).unwrap();
    assert_eq!(consumed1, frame1_len);

    let (t2, consumed2) = Transmission::decode(&stream[consumed1..], &registry).unwrap();
    assert_eq!(consumed1 + consumed2, stream.len());

    // Round-trip the dynamic payloads back to typed for equality checks.
    let p1_back: Sample =
        dots_core::decode_typed_from_slice(&t1.payload.encode()).unwrap();
    let p2_back: Sample =
        dots_core::decode_typed_from_slice(&t2.payload.encode()).unwrap();
    assert_eq!(p1_back, p1);
    assert_eq!(p2_back, p2);
}

#[test]
fn decode_returns_need_more_data_when_body_short() {
    let frame = encode_transmission(&header_for("Sample"), &Sample::default());
    let registry = populated_registry();
    let truncated = &frame[..frame.len() - 1];
    match Transmission::decode(truncated, &registry) {
        Err(FramingError::NeedMoreData { have, need }) => {
            assert_eq!(have, frame.len() - 1);
            assert_eq!(need, frame.len());
        }
        other => panic!("expected NeedMoreData, got {other:?}"),
    }
}

#[test]
fn decode_returns_need_more_data_when_prefix_short() {
    let registry = populated_registry();
    let buf = [SIZE_PREFIX_MARKER, 0, 0];
    match Transmission::decode(&buf, &registry) {
        Err(FramingError::NeedMoreData { have: 3, need: 5 }) => {}
        other => panic!("expected NeedMoreData, got {other:?}"),
    }
}

// ----- Cross-roundtrip: typed encode → dynamic decode → re-encode -----

// ----- encode_into / batching -----

#[test]
fn encode_into_appends_to_existing_buffer() {
    let mut buf = vec![0xAB_u8, 0xCD]; // pre-existing bytes
    encode_transmission_into(
        &header_for("Sample"),
        &Sample {
            id: Some(1),
            label: Some("hi".into()),
        },
        &mut buf,
    );

    // Pre-existing bytes preserved at the front.
    assert_eq!(&buf[..2], &[0xAB, 0xCD]);
    // Frame begins at offset 2 with the size prefix marker.
    assert_eq!(buf[2], SIZE_PREFIX_MARKER);
    let body_size = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]) as usize;
    assert_eq!(2 + SIZE_PREFIX_LEN + body_size, buf.len());
}

#[test]
fn encode_into_batches_multiple_frames() {
    let mut buf = Vec::new();
    let h = header_for("Sample");
    encode_transmission_into(
        &h,
        &Sample {
            id: Some(1),
            ..Default::default()
        },
        &mut buf,
    );
    let after_first = buf.len();
    encode_transmission_into(
        &h,
        &Sample {
            id: Some(2),
            ..Default::default()
        },
        &mut buf,
    );

    // Each frame's size prefix points only at its own body — not the
    // total buffer length.
    assert_eq!(buf[0], SIZE_PREFIX_MARKER);
    let first_body = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    assert_eq!(SIZE_PREFIX_LEN + first_body, after_first);

    assert_eq!(buf[after_first], SIZE_PREFIX_MARKER);
    let second_body = u32::from_be_bytes([
        buf[after_first + 1],
        buf[after_first + 2],
        buf[after_first + 3],
        buf[after_first + 4],
    ]) as usize;
    assert_eq!(after_first + SIZE_PREFIX_LEN + second_body, buf.len());
}

#[test]
fn transmission_encode_into_matches_encode() {
    let registry = populated_registry();
    let typed_frame = encode_transmission(
        &header_for("Sample"),
        &Sample {
            id: Some(7),
            label: Some("equiv".into()),
        },
    );
    let (txn, _) = Transmission::decode(&typed_frame, &registry).unwrap();

    let one_shot = txn.encode();
    let mut buf = Vec::new();
    txn.encode_into(&mut buf);
    assert_eq!(one_shot, buf);
}

#[test]
fn typed_encode_then_dynamic_reencode_is_byte_identical() {
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(31415),
        label: Some("π".into()),
    };
    let typed_frame = encode_transmission(&header, &payload);

    let registry = populated_registry();
    let (txn, _) = Transmission::decode(&typed_frame, &registry).unwrap();
    let dynamic_frame = txn.encode();
    assert_eq!(typed_frame, dynamic_frame);
}

// ----- RawTransmission -----

#[test]
fn raw_transmission_decode_recovers_header_and_payload_slice() {
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(42),
        label: Some("hi".into()),
    };
    let frame_bytes = encode_transmission(&header, &payload);

    let raw = RawTransmission::decode(Bytes::from(frame_bytes.clone())).unwrap();
    assert_eq!(raw.header, header);

    // Re-encoding the original header with the raw payload bytes must
    // reproduce the exact original frame — proving the slice boundaries
    // are correct and `encode_frame_with_header` is byte-symmetric with
    // `encode_transmission_into`.
    let mut rebuilt = Vec::new();
    encode_frame_with_header(&header, &raw.payload, &mut rebuilt);
    assert_eq!(rebuilt, frame_bytes);
}

#[test]
fn raw_transmission_decode_payload_matches_typed() {
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(7),
        label: Some("seven".into()),
    };
    let frame_bytes = encode_transmission(&header, &payload);
    let registry = populated_registry();

    let raw = RawTransmission::decode(Bytes::from(frame_bytes)).unwrap();
    let dyn_payload = raw.decode_payload(&registry).unwrap();
    // Round-trip through Sample to compare semantically.
    let payload_bytes = dyn_payload.encode();
    let recovered: Sample = dots_core::decode_typed_from_slice(&payload_bytes).unwrap();
    assert_eq!(recovered, payload);
}

#[test]
fn raw_transmission_rewrite_header_keeps_payload_bytes() {
    // The whole point: rebuild a frame with a fresh header but the same
    // payload bytes — and have a downstream typed decode of the new
    // frame yield the original payload.
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(99),
        label: Some("verbatim".into()),
    };
    let frame_bytes = encode_transmission(&header, &payload);

    let raw = RawTransmission::decode(Bytes::from(frame_bytes)).unwrap();

    let new_header = DotsHeader {
        type_name: Some("Sample".into()),
        sender: Some(99),
        server_sent_time: Some(dots_core::Timepoint(123.0)),
        ..Default::default()
    };
    let mut rewritten = Vec::new();
    encode_frame_with_header(&new_header, &raw.payload, &mut rewritten);

    let (got_header, got_payload, _) = decode_typed_transmission::<Sample>(&rewritten).unwrap();
    assert_eq!(got_header, new_header);
    assert_eq!(got_payload, payload);
}

#[test]
fn raw_transmission_rejects_short_buffer() {
    let header = header_for("Sample");
    let payload = Sample {
        id: Some(1),
        label: Some("x".into()),
    };
    let frame = encode_transmission(&header, &payload);
    let truncated = Bytes::copy_from_slice(&frame[..frame.len() - 2]);
    match RawTransmission::decode(truncated) {
        Err(FramingError::NeedMoreData { .. }) => {}
        other => panic!("expected NeedMoreData, got {other:?}"),
    }
}
