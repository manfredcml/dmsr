use crate::connector::postgres_source::pgoutput::events::{
    BeginEvent, CommitEvent, InsertEvent, MessageType, PgOutputEvent, RelationColumn,
    RelationEvent, ReplicationIdentity,
};
use crate::error::generic::{DMSRError, DMSRResult};
use byteorder::{BigEndian, ReadBytesExt};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use std::io::{Cursor, Read};

fn read_c_string(cursor: &mut Cursor<&[u8]>) -> DMSRResult<String> {
    let mut buf = Vec::new();
    loop {
        let byte = cursor.read_u8()?;
        if byte == 0 {
            break;
        }
        buf.push(byte);
    }
    let parsed_string = String::from_utf8(buf)?;
    Ok(parsed_string)
}

pub fn parse_pgoutput_event(event: &[u8]) -> DMSRResult<PgOutputEvent> {
    let mut cursor = Cursor::new(event);

    // Skip the 'w' identifier and the lsn info (which will be in the event body)
    cursor.set_position(17);

    let ms_since_2000 = cursor.read_i64::<BigEndian>()?;
    println!("ms_since_2000: {}", ms_since_2000);
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
    let duration = Duration::microseconds(ms_since_2000);
    let timestamp = start_date + duration;

    let message_type = cursor.read_u8()?;
    let message_type = MessageType::from_char(message_type as char)?;

    match message_type {
        MessageType::Relation => {
            let pgoutput = parse_relation_event(timestamp, &mut cursor)?;
            Ok(pgoutput)
        }
        MessageType::Insert => {
            let pgoutput = parse_insert_event(timestamp, &mut cursor)?;
            Ok(pgoutput)
        }
        MessageType::Begin => {
            let pgoutput = parse_begin_event(timestamp, &mut cursor)?;
            Ok(pgoutput)
        }
        MessageType::Commit => {
            let pgoutput = parse_commit_event(timestamp, &mut cursor)?;
            Ok(pgoutput)
        }
    }
}

pub fn parse_commit_event(
    timestamp: NaiveDateTime,
    cursor: &mut Cursor<&[u8]>,
) -> DMSRResult<PgOutputEvent> {
    let flags = cursor.read_u8()?;
    let lsn_commit = cursor.read_u64::<BigEndian>()?;
    let lsn = cursor.read_u64::<BigEndian>()?;

    let ms_since_2000 = cursor.read_i64::<BigEndian>()?;
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
    let duration = Duration::microseconds(ms_since_2000);
    println!("ms_since_2000: {}", ms_since_2000);
    let commit_timestamp = start_date + duration;

    let pgoutput = CommitEvent {
        timestamp,
        flags,
        lsn_commit,
        lsn,
        commit_timestamp,
    };
    Ok(PgOutputEvent::Commit(pgoutput))
}

pub fn parse_begin_event(
    timestamp: NaiveDateTime,
    cursor: &mut Cursor<&[u8]>,
) -> DMSRResult<PgOutputEvent> {
    let lsn = cursor.read_u64::<BigEndian>()?;

    let ms_since_2000 = cursor.read_i64::<BigEndian>()?;
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
    let duration = Duration::microseconds(ms_since_2000);
    println!("ms_since_2000: {}", ms_since_2000);
    let commit_timestamp = start_date + duration;

    let tx_xid = cursor.read_u32::<BigEndian>()?;

    let pgoutput = BeginEvent {
        timestamp,
        lsn,
        commit_timestamp,
        tx_xid,
    };
    Ok(PgOutputEvent::Begin(pgoutput))
}

pub fn parse_relation_event(
    timestamp: NaiveDateTime,
    cursor: &mut Cursor<&[u8]>,
) -> DMSRResult<PgOutputEvent> {
    let namespace_oid = cursor.read_u32::<BigEndian>()?;

    let schema_name = read_c_string(cursor)?;
    let table_name = read_c_string(cursor)?;

    let repl_identity = cursor.read_u8()?;
    let repl_identity = ReplicationIdentity::from_char(repl_identity as char)?;

    let num_columns = cursor.read_u16::<BigEndian>()?;

    let mut columns: Vec<RelationColumn> = vec![];
    for _ in 0..num_columns {
        let is_pk = cursor.read_u8()? == 1;
        let column_name = read_c_string(cursor)?;
        let column_type = cursor.read_u32::<BigEndian>()?;
        let column_type_modifier = cursor.read_i32::<BigEndian>()?;

        let pgoutput_column = RelationColumn {
            is_pk,
            column_name,
            column_type,
            column_type_modifier,
        };
        columns.push(pgoutput_column);
    }

    let pgoutput = RelationEvent {
        timestamp,
        relation_id: namespace_oid,
        namespace: schema_name,
        relation_name: table_name,
        repl_identity,
        num_columns,
        columns,
    };

    Ok(PgOutputEvent::Relation(pgoutput))
}

pub fn parse_insert_event(
    timestamp: NaiveDateTime,
    cursor: &mut Cursor<&[u8]>,
) -> DMSRResult<PgOutputEvent> {
    let relation_id = cursor.read_u32::<BigEndian>()?;
    let tuple_type = cursor.read_u8()? as char;
    let num_columns = cursor.read_u16::<BigEndian>()?;

    let mut values: Vec<String> = vec![];
    for _ in 0..num_columns {
        // "n" for null, "t" for text, "u" for unknown
        let data_category = cursor.read_u8()? as char;
        if data_category == 'n' || data_category == 'u' {
            values.push("NULL".to_string());
            continue;
        }

        let data_length = cursor.read_u32::<BigEndian>()?;
        let mut data = vec![0; data_length as usize];
        cursor.read_exact(&mut data)?;
        let data = String::from_utf8(data)?;
        values.push(data);
    }

    let pgoutput = InsertEvent {
        timestamp,
        relation_id,
        tuple_type,
        num_columns,
        values,
    };
    Ok(PgOutputEvent::Insert(pgoutput))
}

mod tests {
    use super::*;

    #[test]
    fn test_parse_commit() {
        let event = b"w\0\0\0\0\x01Xf\0\0\0\0\0\x01Xf\0\0\x02\x9b\xcat\xc9\x02\xc8C\0\0\0\0\0\x01Xe\xd0\0\0\0\0\x01Xf\0\0\x02\x9b\xcat\xc8\xf7`";
        let event = event as &[u8];

        let pgoutput = parse_pgoutput_event(event);
        assert!(pgoutput.is_ok());

        let event = match pgoutput.unwrap() {
            PgOutputEvent::Commit(event) => event,
            _ => panic!("Expected Relation event"),
        };
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
        let duration = Duration::microseconds(734243798450888);
        let commit_duration = Duration::microseconds(734243798447968);
        let timestamp = start_date + duration;
        let commit_timestamp = start_date + commit_duration;

        assert_eq!(event.timestamp, timestamp);
        assert_eq!(event.flags, 0);
        assert_eq!(event.lsn, 22570496);
        assert_eq!(event.lsn_commit, 22570448);
        assert_eq!(event.commit_timestamp, commit_timestamp);
    }

    #[test]
    fn test_parse_begin() {
        let event = b"w\0\0\0\0\x01XdH\0\0\0\0\x01XdH\0\x02\x9b\xcat\xc8\xfe\xf2B\0\0\0\0\x01Xe\xd0\0\x02\x9b\xcat\xc8\xf7`\0\0\x02\xfa";
        let event = event as &[u8];

        let pgoutput = parse_pgoutput_event(event);
        assert!(pgoutput.is_ok());

        let event = match pgoutput.unwrap() {
            PgOutputEvent::Begin(event) => event,
            _ => panic!("Expected Relation event"),
        };
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
        let duration = Duration::microseconds(734243798449906);
        let commit_duration = Duration::microseconds(734243798447968);
        let timestamp = start_date + duration;
        let commit_timestamp = start_date + commit_duration;

        assert_eq!(event.timestamp, timestamp);
        assert_eq!(event.lsn, 22570448);
        assert_eq!(event.commit_timestamp, commit_timestamp);
        assert_eq!(event.tx_xid, 762);
    }

    #[test]
    fn test_parse_insert() {
        let event = b"w\0\0\0\0\x01Xa\x80\0\0\0\0\x01Xa\x80\0\x02\x9b\xc9\xf2\xd1\x1c\x94I\0\0@\nN\0\x02t\0\0\0\x012t\0\0\0\x04test";
        let event = event as &[u8];

        let pgoutput = parse_pgoutput_event(event);
        assert!(pgoutput.is_ok());

        let event = match pgoutput.unwrap() {
            PgOutputEvent::Insert(event) => event,
            _ => panic!("Expected Relation event"),
        };
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
        let duration = Duration::microseconds(734241617943700);
        let timestamp = start_date + duration;

        assert_eq!(event.timestamp, timestamp);
        assert_eq!(event.relation_id, 16394);
        assert_eq!(event.tuple_type, 'N');
        assert_eq!(event.num_columns, 2);
        assert_eq!(event.values.len(), 2);
        assert_eq!(event.values[0], "2");
        assert_eq!(event.values[1], "test");
    }

    #[test]
    fn test_parse_relation() {
        let event = b"w\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x02\x9b\xbd7\x95\x90\x9fR\0\0@\x04public\0test_tbl\0d\0\x02\x01id\0\0\0\0\x17\xff\xff\xff\xff\0name\0\0\0\x04\x13\0\0\0\x84";
        let event = event as &[u8];
        println!("Event: {:?}", event);

        let pgoutput = parse_pgoutput_event(event);
        assert!(pgoutput.is_ok());

        let pgoutput = match pgoutput.unwrap() {
            PgOutputEvent::Relation(pgoutput) => pgoutput,
            _ => panic!("Expected Relation event"),
        };

        println!("PgOutput: {:?}", pgoutput);
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
        let duration = Duration::microseconds(734186937094303);
        let timestamp = start_date + duration;
        assert_eq!(pgoutput.timestamp, timestamp);

        assert_eq!(pgoutput.relation_id, 16388);
        assert_eq!(pgoutput.namespace, "public");
        assert_eq!(pgoutput.relation_name, "test_tbl");
        assert_eq!(pgoutput.repl_identity, ReplicationIdentity::Default);
        assert_eq!(pgoutput.num_columns, 2);
        assert_eq!(pgoutput.columns.len(), 2);
        assert!(pgoutput.columns[0].is_pk);
        assert_eq!(pgoutput.columns[0].column_name, "id");
        assert_eq!(pgoutput.columns[0].column_type, 23);
        assert_eq!(pgoutput.columns[0].column_type_modifier, -1);
        assert!(!pgoutput.columns[1].is_pk);
        assert_eq!(pgoutput.columns[1].column_name, "name");
        assert_eq!(pgoutput.columns[1].column_type, 1043);
        assert_eq!(pgoutput.columns[1].column_type_modifier, 132);
    }
}
