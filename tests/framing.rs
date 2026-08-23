use fomoxa_net::frame::{
    self, Frame, FrameError, StreamDecoder, MAX_HANDSHAKE_PAYLOAD, MAX_MESSAGE_PAYLOAD,
};

fn encoded_data(id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    frame::encode_data(id, payload, &mut out).unwrap();
    out
}

#[test]
fn data_frame_layout() {
    let bytes = encoded_data(42, &[1, 2, 3]);
    assert_eq!(bytes, [0x00, 0x43, 0x59, 0x2A, 0, 0, 0, 0x03, 0, 0, 0, 1, 2, 3]);
    assert_eq!(bytes.len(), 11 + 3);
    assert_eq!(
        frame::decode_packet(&bytes).unwrap(),
        Frame::Data { message_id: 42, payload: &[1, 2, 3] }
    );
}

#[test]
fn probe_and_ack_are_one_byte() {
    let mut probe = Vec::new();
    frame::encode_probe(&mut probe);
    assert_eq!(probe, [0x01]);
    assert_eq!(frame::decode_packet(&probe).unwrap(), Frame::Probe);

    let mut ack = Vec::new();
    frame::encode_ack(&mut ack);
    assert_eq!(ack, [0x02]);
    assert_eq!(frame::decode_packet(&ack).unwrap(), Frame::Ack);
}

#[test]
fn handshake_frame_layout() {
    let mut bytes = Vec::new();
    frame::encode_handshake(&[0xAA, 0xBB], &mut bytes).unwrap();
    assert_eq!(bytes, [0x03, 0x02, 0, 0, 0, 0xAA, 0xBB]);
    assert_eq!(frame::decode_packet(&bytes).unwrap(), Frame::Handshake(&[0xAA, 0xBB]));
}

#[test]
fn a_stream_fed_one_byte_at_a_time_still_yields_frames() {
    let mut wire = Vec::new();
    wire.extend_from_slice(&encoded_data(7, b"hello"));
    frame::encode_probe(&mut wire);
    frame::encode_handshake(&[1, 2, 3], &mut wire).unwrap();
    frame::encode_ack(&mut wire);

    let mut decoder = StreamDecoder::new();
    let mut seen = Vec::new();
    for byte in &wire {
        decoder.feed(&[*byte]);
        while let Some(len) = decoder.next_len().unwrap() {
            let frame = decoder.frame(len).unwrap();
            seen.push(match frame {
                Frame::Data { message_id, payload } => format!("data {message_id} {payload:?}"),
                Frame::Probe => "probe".to_owned(),
                Frame::Ack => "ack".to_owned(),
                Frame::Handshake(payload) => format!("handshake {payload:?}"),
            });
            decoder.advance(len);
        }
    }

    assert_eq!(
        seen,
        vec![
            "data 7 [104, 101, 108, 108, 111]".to_owned(),
            "probe".to_owned(),
            "handshake [1, 2, 3]".to_owned(),
            "ack".to_owned(),
        ]
    );
}

#[test]
fn several_frames_in_one_feed_are_split() {
    let mut wire = Vec::new();
    frame::encode_probe(&mut wire);
    wire.extend_from_slice(&encoded_data(1, b"a"));
    frame::encode_ack(&mut wire);

    let mut decoder = StreamDecoder::new();
    decoder.feed(&wire);

    let mut count = 0;
    while let Some(len) = decoder.next_len().unwrap() {
        decoder.frame(len).unwrap();
        decoder.advance(len);
        count += 1;
    }
    assert_eq!(count, 3);
    assert_eq!(decoder.buffered(), 0);
}

#[test]
fn a_broken_stream_stays_broken() {
    let mut decoder = StreamDecoder::new();
    decoder.feed(&[0x09]);
    assert_eq!(decoder.next_len(), Err(FrameError::UnknownType(0x09)));
    decoder.feed(&encoded_data(1, b"a"));
    assert_eq!(decoder.next_len(), Err(FrameError::UnknownType(0x09)));
    assert!(decoder.is_poisoned());
}

#[test]
fn bad_magic_is_caught_before_the_header_is_complete() {
    let mut decoder = StreamDecoder::new();
    decoder.feed(&[0x00, 0x43, 0x5A]);
    assert_eq!(decoder.next_len(), Err(FrameError::BadMagic([0x43, 0x5A])));
}

#[test]
fn a_packet_holds_exactly_one_frame() {
    let mut short = encoded_data(1, b"abc");
    short.pop();
    assert_eq!(frame::decode_packet(&short), Err(FrameError::Truncated));

    let mut long = encoded_data(1, b"abc");
    long.push(0x01);
    assert_eq!(frame::decode_packet(&long), Err(FrameError::Trailing(1)));

    assert_eq!(frame::decode_packet(&[]), Err(FrameError::Truncated));
}

#[test]
fn an_unknown_type_byte_is_rejected_in_a_packet() {
    assert_eq!(frame::decode_packet(&[0x04]), Err(FrameError::UnknownType(0x04)));
}

#[test]
fn the_message_cap_is_enforced_on_both_sides() {
    let over = vec![0u8; MAX_MESSAGE_PAYLOAD + 1];
    let mut out = Vec::new();
    assert_eq!(
        frame::encode_data(1, &over, &mut out),
        Err(FrameError::MessageTooLarge(MAX_MESSAGE_PAYLOAD as u64 + 1))
    );

    let mut header = vec![0x00, 0x43, 0x59, 0, 0, 0, 0];
    header.extend_from_slice(&((MAX_MESSAGE_PAYLOAD + 1) as u32).to_le_bytes());
    assert_eq!(
        frame::decode(&header),
        Err(FrameError::MessageTooLarge(MAX_MESSAGE_PAYLOAD as u64 + 1))
    );

    let mut at_cap = vec![0x00, 0x43, 0x59, 0, 0, 0, 0];
    at_cap.extend_from_slice(&(MAX_MESSAGE_PAYLOAD as u32).to_le_bytes());
    assert_eq!(frame::decode(&at_cap), Ok(None));
}

#[test]
fn the_handshake_cap_is_enforced_on_both_sides() {
    let over = vec![0u8; MAX_HANDSHAKE_PAYLOAD + 1];
    let mut out = Vec::new();
    assert_eq!(
        frame::encode_handshake(&over, &mut out),
        Err(FrameError::HandshakeTooLarge(MAX_HANDSHAKE_PAYLOAD as u64 + 1))
    );

    let mut header = vec![0x03];
    header.extend_from_slice(&((MAX_HANDSHAKE_PAYLOAD + 1) as u32).to_le_bytes());
    assert_eq!(
        frame::decode(&header),
        Err(FrameError::HandshakeTooLarge(MAX_HANDSHAKE_PAYLOAD as u64 + 1))
    );
}

#[test]
fn an_empty_payload_is_a_valid_data_frame() {
    let bytes = encoded_data(0, &[]);
    assert_eq!(bytes, [0x00, 0x43, 0x59, 0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(frame::decode_packet(&bytes).unwrap(), Frame::Data { message_id: 0, payload: &[] });
}
