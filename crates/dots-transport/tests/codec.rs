//! Async round-trip tests for `TransmissionCodec` using `tokio::io::duplex`.
//!
//! `duplex` gives us a bidirectional in-memory pipe — perfect for
//! exercising the full encode → write → read → decode path without
//! involving real sockets. The same `Framed<S, TransmissionCodec>`
//! shape works over any `AsyncRead+AsyncWrite` stream, so these tests
//! exercise exactly what production TCP / UDS deployments will do.

use std::sync::Arc;

use bytes::BytesMut;
use dots_core::{DynamicStruct, decode_typed_from_slice, dots, encode_to_vec};
use dots_derive::DotsStruct;
use dots_model::{
    DotsHeader, FramingError, MAX_BODY_SIZE, Registry, SIZE_PREFIX_MARKER, StructDescriptorData,
    Transmission,
};
use dots_transport::{TransmissionCodec, TransportError};
use futures_util::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio_util::codec::{Decoder, Encoder, Framed};

#[derive(DotsStruct, Default, Debug, PartialEq, Clone)]
#[dots(name = "Sample", cached)]
struct Sample {
    #[dots(tag = 1, key)]
    id: Option<u32>,
    #[dots(tag = 2)]
    label: Option<String>,
}

fn populated_registry() -> Arc<Registry> {
    let reg = Registry::new();
    let data = StructDescriptorData::from_static(Sample::DESCRIPTOR);
    let dyn_desc = reg.build_dynamic_struct(&data).unwrap();
    reg.register_struct_dynamic(Arc::new(dyn_desc));
    Arc::new(reg)
}

fn sample_transmission(id: u32, label: &str) -> Transmission {
    let header = dots!(DotsHeader {
        type_name: "Sample",
        sender: 7_u32,
    });
    // Build the dynamic payload by typed-encoding then registry-decoding.
    let typed = dots!(Sample {
        id: id,
        label: label,
    });
    let payload_bytes = encode_to_vec(&typed);
    let registry = populated_registry();
    let descriptor = match registry.lookup("Sample").unwrap() {
        dots_model::DescriptorEntry::Struct(d) => d.clone(),
        _ => panic!(),
    };
    let payload = DynamicStruct::decode(descriptor, &payload_bytes).unwrap();
    Transmission { header, payload: dots_model::Payload::Wire(payload) }
}

// ----- Synchronous codec exercises (no futures) -----

#[test]
fn decoder_returns_none_when_buffer_empty() {
    let mut codec = TransmissionCodec::new(populated_registry());
    let mut buf = BytesMut::new();
    assert!(matches!(codec.decode(&mut buf), Ok(None)));
}

#[test]
fn decoder_returns_none_when_only_partial_prefix_present() {
    let mut codec = TransmissionCodec::new(populated_registry());
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&[SIZE_PREFIX_MARKER, 0, 0]); // 3 < 5
    assert!(matches!(codec.decode(&mut buf), Ok(None)));
    // Buffer should still contain the bytes — codec doesn't consume on incomplete.
    assert_eq!(buf.len(), 3);
}

#[test]
fn decoder_consumes_one_frame_and_leaves_remainder() {
    let txn1 = sample_transmission(1, "first");
    let txn2 = sample_transmission(2, "second");

    let mut buf = BytesMut::new();
    buf.extend_from_slice(&txn1.encode());
    let frame1_end = buf.len();
    buf.extend_from_slice(&txn2.encode());

    let mut codec = TransmissionCodec::new(populated_registry());
    let decoded1 = codec.decode(&mut buf).unwrap().expect("first frame");
    assert_eq!(decoded1.header.sender, txn1.header.sender);
    assert_eq!(buf.len(), buf.capacity().min(buf.len()));
    // Remainder is exactly the bytes of frame 2.
    let txn1_len = frame1_end;
    let total_initial = txn1_len + txn2.encode().len();
    assert_eq!(buf.len() + txn1_len, total_initial);

    let decoded2 = codec.decode(&mut buf).unwrap().expect("second frame");
    assert_eq!(decoded2.header.sender, txn2.header.sender);
    assert_eq!(buf.len(), 0);
}

#[test]
fn decoder_surfaces_invalid_prefix_error() {
    let mut codec = TransmissionCodec::new(populated_registry());
    let mut buf = BytesMut::from(&[0xFF_u8, 0, 0, 0, 1][..]);
    match codec.decode(&mut buf) {
        Err(TransportError::Framing(FramingError::InvalidSizePrefix(0xFF))) => {}
        other => panic!("expected InvalidSizePrefix, got {other:?}"),
    }
}

#[test]
fn decoder_surfaces_oversize_error() {
    let mut codec = TransmissionCodec::new(populated_registry());
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&[SIZE_PREFIX_MARKER]);
    buf.extend_from_slice(&(MAX_BODY_SIZE + 1).to_be_bytes());
    match codec.decode(&mut buf) {
        Err(TransportError::Framing(FramingError::BodyTooLarge { size })) => {
            assert_eq!(size, MAX_BODY_SIZE + 1);
        }
        other => panic!("expected BodyTooLarge, got {other:?}"),
    }
}

#[test]
fn encoder_appends_full_frame_to_buffer() {
    let mut codec = TransmissionCodec::new(populated_registry());
    let mut buf = BytesMut::new();
    let txn = sample_transmission(42, "encoded");
    codec.encode(txn.clone(), &mut buf).unwrap();

    let expected = txn.encode();
    assert_eq!(buf.as_ref(), expected.as_slice());
}

// ----- Async round-trips via tokio::io::duplex -----

#[tokio::test]
async fn framed_pair_round_trips_a_transmission() {
    let registry = populated_registry();
    let codec = TransmissionCodec::new(registry);

    let (a, b) = tokio::io::duplex(1024);
    let mut sender = Framed::new(a, codec.clone());
    let mut receiver = Framed::new(b, codec);

    let txn = sample_transmission(101, "hello");
    sender.send(txn.clone()).await.unwrap();

    let received = receiver.next().await.expect("frame arrives").unwrap();
    assert_eq!(received.header, txn.header);
    // Re-encoding both must produce identical bytes.
    assert_eq!(received.encode(), txn.encode());
}

#[tokio::test]
async fn framed_pair_streams_multiple_transmissions_in_order() {
    let registry = populated_registry();
    let codec = TransmissionCodec::new(registry);

    let (a, b) = tokio::io::duplex(4096);
    let mut sender = Framed::new(a, codec.clone());
    let mut receiver = Framed::new(b, codec);

    let txns = vec![
        sample_transmission(1, "a"),
        sample_transmission(2, "b"),
        sample_transmission(3, "c"),
    ];
    let send_task = {
        let txns = txns.clone();
        tokio::spawn(async move {
            for txn in txns {
                sender.send(txn).await.unwrap();
            }
            // Disambiguate Sink::close — there's now both a Sink<Transmission>
            // and Sink<Vec<u8>> impl on TransmissionCodec.
            SinkExt::<Transmission>::close(&mut sender).await.unwrap();
        })
    };

    let mut received = Vec::new();
    while let Some(item) = receiver.next().await {
        received.push(item.unwrap());
    }
    send_task.await.unwrap();

    assert_eq!(received.len(), txns.len());
    for (got, want) in received.iter().zip(txns.iter()) {
        assert_eq!(got.encode(), want.encode());
    }
}

#[tokio::test]
async fn decoder_assembles_frame_from_chunked_writes() {
    // Writes the bytes of one frame in two halves, with a brief sleep
    // between, to confirm Framed buffers correctly across reads.
    let registry = populated_registry();
    let codec = TransmissionCodec::new(registry);

    let (mut a, b) = tokio::io::duplex(1024);
    let mut receiver = Framed::new(b, codec);

    let txn = sample_transmission(7, "chunked");
    let bytes = txn.encode();
    let split = bytes.len() / 2;

    let writer = tokio::spawn(async move {
        a.write_all(&bytes[..split]).await.unwrap();
        // Yield so the receiver task can observe the partial write
        // and exercise its NeedMoreData branch before the rest arrives.
        tokio::task::yield_now().await;
        a.write_all(&bytes[split..]).await.unwrap();
        a.shutdown().await.unwrap();
    });

    let received = receiver.next().await.expect("frame arrives").unwrap();
    writer.await.unwrap();

    assert_eq!(received.encode(), txn.encode());
}

#[tokio::test]
async fn decoder_surfaces_unknown_type_for_payload_not_in_registry() {
    // Sender's registry knows Sample; receiver's registry doesn't.
    let sender_registry = populated_registry();
    let receiver_registry = Arc::new(Registry::new());

    let (a, b) = tokio::io::duplex(1024);
    let mut sender = Framed::new(a, TransmissionCodec::new(sender_registry));
    let mut receiver = Framed::new(b, TransmissionCodec::new(receiver_registry));

    sender
        .send(sample_transmission(1, "hi"))
        .await
        .unwrap();

    match receiver.next().await {
        Some(Err(TransportError::Framing(FramingError::UnknownType(name)))) => {
            assert_eq!(name, "Sample");
        }
        other => panic!("expected UnknownType, got {other:?}"),
    }
}

#[tokio::test]
async fn typed_payload_decodes_via_registry_then_back_to_typed() {
    // Sender uses encode_transmission; receiver gets a dynamic
    // Transmission, re-encodes the payload, decodes back to typed Sample.
    // Proves the codec is a transparent path for typed/dynamic mix.
    let registry = populated_registry();

    let (mut a, b) = tokio::io::duplex(1024);
    let mut receiver = Framed::new(b, TransmissionCodec::new(registry));

    let header = dots!(DotsHeader {
        type_name: "Sample",
        sender: 99_u32,
    });
    let typed_payload = dots!(Sample {
        id: 1234_u32,
        label: "typed→dynamic→typed",
    });
    let frame = dots_model::encode_transmission(&header, &typed_payload);

    a.write_all(&frame).await.unwrap();
    a.shutdown().await.unwrap();

    let txn = receiver.next().await.expect("frame arrives").unwrap();
    assert_eq!(txn.header.sender, Some(99));

    let payload_bytes = txn.payload.encode();
    let back: Sample = decode_typed_from_slice(&payload_bytes).unwrap();
    assert_eq!(back, typed_payload);
}
