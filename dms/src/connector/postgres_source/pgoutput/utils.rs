use crate::connector::postgres_source::pgoutput::events::{
    BeginEvent, ColumnData, ColumnDataCategory, CommitEvent, DeleteEvent, InsertEvent, MessageType,
    PgOutputEvent, RelationColumn, RelationEvent, ReplicationIdentity, TupleType, UpdateEvent,
};
use crate::error::generic::{DMSRError, DMSRResult};
use byteorder::{BigEndian, ReadBytesExt};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use std::io::{Cursor, Read};
use std::str::FromStr;

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

fn parse_timestamp(ms_since_2000: i64) -> NaiveDateTime {
    println!("ms_since_2000: {}", ms_since_2000);
    let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
    let duration = Duration::microseconds(ms_since_2000);
    start_date + duration
}

fn parse_u8_into_enum<T>(val: u8) -> DMSRResult<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let val = &[val];
    let val = std::str::from_utf8(val)?;
    let val = T::from_str(val).map_err(|e| DMSRError::StrumParseError(e.to_string()))?;
    Ok(val)
}

fn read_columns(num_columns: u16, cursor: &mut Cursor<&[u8]>) -> DMSRResult<Vec<ColumnData>> {
    let mut columns: Vec<ColumnData> = vec![];

    for _ in 0..num_columns {
        let col_data_category = parse_u8_into_enum::<ColumnDataCategory>(cursor.read_u8()?)?;

        match col_data_category {
            ColumnDataCategory::Null => {
                columns.push(ColumnData {
                    column_data_category: ColumnDataCategory::Null,
                    column_data_length: None,
                    column_value: None,
                });
            }
            ColumnDataCategory::Unknown => {
                columns.push(ColumnData {
                    column_data_category: ColumnDataCategory::Unknown,
                    column_data_length: None,
                    column_value: None,
                });
            }
            ColumnDataCategory::Text => {
                let col_data_length = cursor.read_u32::<BigEndian>()?;
                let mut col_data = vec![0; col_data_length as usize];
                cursor.read_exact(&mut col_data)?;
                let col_value = String::from_utf8(col_data)?;
                columns.push(ColumnData {
                    column_data_category: ColumnDataCategory::Text,
                    column_data_length: Some(col_data_length),
                    column_value: Some(col_value),
                });
            }
        }
    }

    Ok(columns)
}

pub fn parse_pgoutput_event(event: &[u8]) -> DMSRResult<PgOutputEvent> {
    let mut cursor = Cursor::new(event);

    // Skip the 'w' identifier and the lsn info (which will be in the event body)
    cursor.set_position(17);

    let ms_since_2000 = cursor.read_i64::<BigEndian>()?;
    let timestamp = parse_timestamp(ms_since_2000);

    let message_type = parse_u8_into_enum::<MessageType>(cursor.read_u8()?)?;

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
        MessageType::Update => {
            let pgoutput = parse_update_event(timestamp, &mut cursor)?;
            Ok(pgoutput)
        }
        MessageType::Delete => {
            let pgoutput = parse_delete_event(timestamp, &mut cursor)?;
            Ok(pgoutput)
        }
    }
}

pub fn parse_delete_event(
    timestamp: NaiveDateTime,
    cursor: &mut Cursor<&[u8]>,
) -> DMSRResult<PgOutputEvent> {
    let relation_id = cursor.read_u32::<BigEndian>()?;
    let tuple_type = parse_u8_into_enum::<TupleType>(cursor.read_u8()?)?;

    if tuple_type == TupleType::New {
        return Err(DMSRError::PostgresError(
            "Delete event with non-key tuple".to_string(),
        ));
    }

    let num_columns = cursor.read_u16::<BigEndian>()?;
    let columns = read_columns(num_columns, cursor)?;

    let pgoutput = DeleteEvent {
        timestamp,
        tuple_type,
        relation_id,
        columns,
        num_columns,
    };

    Ok(PgOutputEvent::Delete(pgoutput))
}

pub fn parse_update_event(
    timestamp: NaiveDateTime,
    cursor: &mut Cursor<&[u8]>,
) -> DMSRResult<PgOutputEvent> {
    let relation_id = cursor.read_u32::<BigEndian>()?;

    let tuple_type = parse_u8_into_enum::<TupleType>(cursor.read_u8()?)?;

    match tuple_type {
        TupleType::Key => {
            return Err(DMSRError::UnimplementedError(
                "Update event with key tuple".to_string(),
            ));
        }
        TupleType::Old => {
            return Err(DMSRError::UnimplementedError(
                "Update event with old tuple".to_string(),
            ));
        }
        TupleType::New => {}
    }

    let num_columns = cursor.read_u16::<BigEndian>()?;
    let columns = read_columns(num_columns, cursor)?;

    let pgoutput = UpdateEvent {
        timestamp,
        relation_id,
        tuple_type,
        num_columns,
        columns,
    };
    Ok(PgOutputEvent::Update(pgoutput))
}

pub fn parse_commit_event(
    timestamp: NaiveDateTime,
    cursor: &mut Cursor<&[u8]>,
) -> DMSRResult<PgOutputEvent> {
    let flags = cursor.read_u8()?;
    let lsn_commit = cursor.read_u64::<BigEndian>()?;
    let lsn = cursor.read_u64::<BigEndian>()?;

    let ms_since_2000 = cursor.read_i64::<BigEndian>()?;
    let commit_timestamp = parse_timestamp(ms_since_2000);

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
    let commit_timestamp = parse_timestamp(ms_since_2000);

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

    let repl_identity = parse_u8_into_enum::<ReplicationIdentity>(cursor.read_u8()?)?;
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

    let tuple_type = parse_u8_into_enum::<TupleType>(cursor.read_u8()?)?;

    let num_columns = cursor.read_u16::<BigEndian>()?;
    let columns = read_columns(num_columns, cursor)?;

    let pgoutput = InsertEvent {
        timestamp,
        relation_id,
        tuple_type,
        num_columns,
        columns,
    };
    Ok(PgOutputEvent::Insert(pgoutput))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_delete() {
        let event =
            b"w\0\0\0\0\x01Xp`\0\0\0\0\x01Xp`\0\x02\x9b\xd0.(\xeb6D\0\0@\nK\0\x02t\0\0\0\x011n";
        let event = event as &[u8];

        let pgoutput = parse_pgoutput_event(event);
        assert!(pgoutput.is_ok());

        let event = match pgoutput.unwrap() {
            PgOutputEvent::Delete(event) => event,
            _ => panic!("Expected Delete event"),
        };
        let timestamp = parse_timestamp(734268383357750);

        assert_eq!(event.timestamp, timestamp);
        assert_eq!(event.relation_id, 16394);
        assert_eq!(event.tuple_type, TupleType::Key);
        assert_eq!(event.num_columns, 2);
        assert_eq!(event.columns.len(), 2);
        assert_eq!(event.columns[0].column_data_category, ColumnDataCategory::Text);
        assert_eq!(event.columns[0].column_data_length, Some(1));
        assert_eq!(event.columns[0].column_value, Some("1".to_string()));
        assert_eq!(event.columns[1].column_data_category, ColumnDataCategory::Null);
        assert_eq!(event.columns[1].column_data_length, None);
        assert_eq!(event.columns[1].column_value, None);
    }

    #[test]
    fn test_parse_update() {
        let event = b"w\0\0\0\0\x01Xn\xc0\0\0\0\0\x01Xn\xc0\0\x02\x9b\xcf0\xc5\0\x12U\0\0@\nN\0\x02t\0\0\0\x011t\0\0\0\x05test3";
        let event = event as &[u8];

        let pgoutput = parse_pgoutput_event(event);
        assert!(pgoutput.is_ok());

        let event = match pgoutput.unwrap() {
            PgOutputEvent::Update(event) => event,
            _ => panic!("Expected Relation event"),
        };
        let timestamp = parse_timestamp(734264132173842);

        assert_eq!(event.timestamp, timestamp);
        assert_eq!(event.relation_id, 16394);
        assert_eq!(event.tuple_type, TupleType::New);
        assert_eq!(event.num_columns, 2);
        assert_eq!(event.columns.len(), 2);
        assert_eq!(
            event.columns[0].column_data_category,
            ColumnDataCategory::Text
        );
        assert_eq!(event.columns[0].column_data_length, Some(1));
        assert_eq!(event.columns[0].column_value, Some("1".to_string()));

        assert_eq!(
            event.columns[1].column_data_category,
            ColumnDataCategory::Text
        );
        assert_eq!(event.columns[1].column_data_length, Some(5));
        assert_eq!(event.columns[1].column_value, Some("test3".to_string()));
    }

    #[test]
    fn test_parse_commit() {
        let event = b"w\0\0\0\0\x01Xf\0\0\0\0\0\x01Xf\0\0\x02\x9b\xcat\xc9\x02\xc8C\0\0\0\0\0\x01Xe\xd0\0\0\0\0\x01Xf\0\0\x02\x9b\xcat\xc8\xf7`";
        let event = event as &[u8];

        let pgoutput = parse_pgoutput_event(event);
        assert!(pgoutput.is_ok());

        let event = match pgoutput.unwrap() {
            PgOutputEvent::Commit(event) => event,
            _ => panic!("Expected Commit event"),
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
            _ => panic!("Expected Begin event"),
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
            _ => panic!("Expected Insert event"),
        };
        let start_date = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let start_date = start_date.and_hms_opt(0, 0, 0).unwrap();
        let duration = Duration::microseconds(734241617943700);
        let timestamp = start_date + duration;

        assert_eq!(event.timestamp, timestamp);
        assert_eq!(event.relation_id, 16394);
        assert_eq!(event.tuple_type, TupleType::New);
        assert_eq!(event.num_columns, 2);
        assert_eq!(event.columns.len(), 2);
        assert_eq!(event.columns[0].column_value, Some("2".to_string()));
        assert_eq!(event.columns[1].column_value, Some("test".to_string()));
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
