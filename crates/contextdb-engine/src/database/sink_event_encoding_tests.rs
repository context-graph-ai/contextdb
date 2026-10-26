use super::*;

#[test]
fn action_identity_survives_external_json_and_binary_encoding() {
    let event = SinkEvent {
        action_id: 17,
        event_type: "notice".to_string(),
        table: "notifications".to_string(),
        row_values: HashMap::new(),
        severity: "warning".to_string(),
        at_lsn: Lsn(9),
    };
    let previous_bytes =
        bincode::serde::encode_to_vec(&event, bincode::config::standard()).unwrap();
    let (decoded, _): (SinkEvent, usize) =
        bincode::serde::decode_from_slice(&previous_bytes, bincode::config::standard())
            .unwrap();
    assert_eq!(decoded.action_id, 17);
    assert_eq!(decoded.event_type, event.event_type);
    assert_eq!(serde_json::to_value(&event).unwrap()["action_id"], 17);
}
