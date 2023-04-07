use crate::error::generic::{DMSRError, DMSRResult};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use std::io::{Cursor, Read};

#[derive(Debug, PartialEq)]
pub enum PgOutputMessageType {
    Relation,
    Insert,
}

impl PgOutputMessageType {
    pub fn from_char(c: char) -> DMSRResult<Self> {
        match c {
            'R' => Ok(PgOutputMessageType::Relation),
            'I' => Ok(PgOutputMessageType::Insert),
            _ => Err(DMSRError::PostgresError("Unknown message type".into())),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum PgOutputReplicationIdentity {
    Default,
}

impl From<char> for PgOutputReplicationIdentity {
    fn from(c: char) -> Self {
        match c {
            'd' => PgOutputReplicationIdentity::Default,
            _ => panic!("Unknown replication identity"),
        }
    }
}

#[derive(Debug)]
pub struct PgOutputColumn {
    is_pk: bool,
    column_name: String,
    column_type: u32,
    column_type_modifier: i32,
}

#[derive(Debug)]
pub struct RelationEvent {
    lsn: u64,
    timestamp: u128,
    namespace_oid: u32,
    schema_name: String,
    table_name: String,
    repl_identity: PgOutputReplicationIdentity,
    num_columns: u16,
    columns: Vec<PgOutputColumn>,
}

#[derive(Debug)]
pub struct InsertEvent {
    lsn: u64,
    timestamp: u128,
    num_columns: u16,
    values: Vec<String>,
}

#[derive(Debug)]
pub enum PgOutputEvent {
    Relation(RelationEvent),
    Insert(InsertEvent),
}

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
    cursor.read_u8()?;

    let mut lsn = [0u8; 8];
    cursor.read_exact(&mut lsn)?;
    let lsn = u64::from_be_bytes(lsn);

    let mut timestamp = [0u8; 16];
    cursor.read_exact(&mut timestamp)?;
    let timestamp = u128::from_be_bytes(timestamp);

    let message_type = cursor.read_u8()?;
    let message_type = PgOutputMessageType::from_char(message_type as char)?;

    match message_type {
        PgOutputMessageType::Relation => {
            let pgoutput = parse_relation_event(lsn, timestamp, &mut cursor)?;
            Ok(pgoutput)
        }
        PgOutputMessageType::Insert => {
            let pgoutput = parse_insert_event(lsn, timestamp, &mut cursor)?;
            Ok(pgoutput)
        }
    }
}

pub fn parse_relation_event(
    lsn: u64,
    timestamp: u128,
    cursor: &mut Cursor<&[u8]>,
) -> DMSRResult<PgOutputEvent> {
    let namespace_oid = cursor.read_u32::<BigEndian>()?;

    let schema_name = read_c_string(cursor)?;
    let table_name = read_c_string(cursor)?;

    let repl_identity = cursor.read_u8()?;
    let repl_identity: PgOutputReplicationIdentity = (repl_identity as char).into();

    let num_columns = cursor.read_u16::<BigEndian>()?;

    let mut columns: Vec<PgOutputColumn> = vec![];
    for _ in 0..num_columns {
        let is_pk = cursor.read_u8()? == 1;
        let column_name = read_c_string(cursor)?;
        let column_type = cursor.read_u32::<BigEndian>()?;
        let column_type_modifier = cursor.read_i32::<BigEndian>()?;

        let pgoutput_column = PgOutputColumn {
            is_pk,
            column_name,
            column_type,
            column_type_modifier,
        };
        columns.push(pgoutput_column);
    }

    let pgoutput = RelationEvent {
        lsn,
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
    lsn: u64,
    timestamp: u128,
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
        lsn,
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
        let event = b"w\0\0\0\0\x01V\\\xb0\0\0\0\0\x01V\\\xb0\0\x02\x9b\xbd7\x95\x91\x1dI\0\0@\x04N\0\x02t\0\0\0\x0214t\0\0\0\x04test";
        let event = event as &[u8];

        let pgoutput = parse_pgoutput_event(event);
        assert!(pgoutput.is_ok());

        let event = match pgoutput.unwrap() {
            PgOutputEvent::Insert(event) => event,
            _ => panic!("Expected Relation event"),
        };
        assert_eq!(event.lsn, 22437040);
        assert_eq!(event.timestamp, 413890334652318344927351069);
        assert_eq!(event.num_columns, 2);
        assert_eq!(event.values.len(), 2);
        assert_eq!(event.values[0], "14");
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
        assert_eq!(pgoutput.lsn, 0);
        assert_eq!(pgoutput.timestamp, 734186937094303);
        assert_eq!(pgoutput.namespace_oid, 16388);
        assert_eq!(pgoutput.schema_name, "public");
        assert_eq!(pgoutput.table_name, "test_tbl");
        assert_eq!(pgoutput.repl_identity, PgOutputReplicationIdentity::Default);
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
