use crate::connector::postgres_source::pgoutput::events::{
    InsertEvent, MessageType, PgOutputEvent, RelationColumn, RelationEvent, ReplicationIdentity,
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
    }
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
        namespace_oid,
        schema_name,
        table_name,
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
    let sth = cursor.read_u8()?;
    let relation_id = cursor.read_u32::<BigEndian>()?;
    let num_columns = cursor.read_u16::<BigEndian>()?;

    let mut values: Vec<String> = vec![];
    for _ in 0..num_columns {
        let data_type = cursor.read_u8()? as char;
        let data_length = cursor.read_u32::<BigEndian>()?;
        let mut data = vec![0u8; data_length as usize];
        cursor.read_exact(&mut data)?;
        let data = String::from_utf8(data)?;
        values.push(data);
    }

    let pgoutput = InsertEvent {
        timestamp,
        num_columns,
        values,
    };
    Ok(PgOutputEvent::Insert(pgoutput))
}

mod tests {
    use super::*;

    #[test]
    fn test_parse_insert() {
        let event = b"w\0\0\0\0\x01Xa\x80\0\0\0\0\x01Xa\x80\0\x02\x9b\xc9\xf2\xd1\x1c\x94I\0\0@\nN\0\x02t\0\0\0\x012t\0\0\0\x04test";
        let event = event as &[u8];
        println!("{:?}", event);

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

        assert_eq!(pgoutput.namespace_oid, 16388);
        assert_eq!(pgoutput.schema_name, "public");
        assert_eq!(pgoutput.table_name, "test_tbl");
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
