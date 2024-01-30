use parking_lot::Mutex;
use sqlx::{postgres, sqlite, Pool, Postgres, Sqlite};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

mod time {
    use serde::{Serialize, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[derive(Copy, Clone, Debug)]
    pub struct Timestamp(Duration);

    impl Timestamp {
        pub fn now() -> Self {
            Self(SystemTime::now().duration_since(UNIX_EPOCH).unwrap())
        }
        pub fn before24h() -> Self {
            Self(Self::now().0 - Duration::from_secs(86400))
        }
        pub fn as_micros(self) -> u128 {
            self.0.as_micros()
        }
        pub fn as_nanos(self) -> u128 {
            self.0.as_nanos()
        }
    }
    impl Serialize for Timestamp {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_u128(self.as_nanos())
        }
    }

    mod impl_time_db {
        use super::Timestamp;
        use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
        use sqlx::sqlite::{SqliteArgumentValue, SqliteTypeInfo, SqliteValueRef};
        use sqlx::{encode::IsNull, error::BoxDynError, Decode, Encode, Postgres, Sqlite, Type};
        use std::time::Duration;

        impl Type<Sqlite> for Timestamp {
            fn type_info() -> SqliteTypeInfo {
                <i64 as Type<Sqlite>>::type_info()
            }

            fn compatible(ty: &SqliteTypeInfo) -> bool {
                *ty == <i64 as Type<Sqlite>>::type_info()
                    || *ty == <i32 as Type<Sqlite>>::type_info()
                    || *ty == <i16 as Type<Sqlite>>::type_info()
                    || *ty == <i8 as Type<Sqlite>>::type_info()
            }
        }

        impl<'q> Encode<'q, Sqlite> for Timestamp {
            fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
                args.push(SqliteArgumentValue::Int64(
                    i64::try_from(self.as_nanos()).expect("timestamp too large"),
                ));

                IsNull::No
            }
        }

        impl<'r> Decode<'r, Sqlite> for Timestamp {
            fn decode(value: SqliteValueRef<'r>) -> Result<Self, BoxDynError> {
                let value = <i64 as Decode<Sqlite>>::decode(value)?;
                Ok(Timestamp(Duration::from_nanos(
                    value.try_into().unwrap_or_default(),
                )))
            }
        }

        impl Type<Postgres> for Timestamp {
            fn type_info() -> PgTypeInfo {
                PgTypeInfo::with_name("TIMESTAMPTZ")
            }
            fn compatible(ty: &PgTypeInfo) -> bool {
                *ty == PgTypeInfo::with_name("TIMESTAMPTZ")
                    || *ty == PgTypeInfo::with_name("TIMESTAMP")
            }
        }

        const J2000_EPOCH_US: i64 = 946_684_800_000_000;

        impl Encode<'_, Postgres> for Timestamp {
            fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> IsNull {
                let us =
                    i64::try_from(self.as_micros()).expect("timestamp too large") - J2000_EPOCH_US;
                Encode::<Postgres>::encode(us, buf)
            }

            fn size_hint(&self) -> usize {
                std::mem::size_of::<i64>()
            }
        }

        impl<'r> Decode<'r, Postgres> for Timestamp {
            fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
                let us: i64 = Decode::<Postgres>::decode(value)?;
                Ok(Timestamp(Duration::from_micros(
                    (us + J2000_EPOCH_US).try_into().unwrap_or_default(),
                )))
            }
        }
    }
}
use time::Timestamp;

mod event {
    use super::time::Timestamp;
    use serde::Serialize;
    use uuid::Uuid;

    #[derive(sqlx::FromRow, Debug, Serialize)]
    pub struct Event {
        pub(crate) id: Uuid,
        pub(crate) sensor: String,
        pub(crate) value: f64,
        pub(crate) t: Timestamp,
    }

    impl Event {
        pub fn new<S>(sensor: S, value: f64) -> Self
        where
            S: std::fmt::Display,
        {
            Self {
                id: Uuid::new_v4(),
                sensor: sensor.to_string(),
                value,
                t: Timestamp::now(),
            }
        }
    }
}
use event::Event;

mod buffer {
    use parking_lot::Mutex;
    use std::mem;

    pub struct Buffer<T> {
        buf: Mutex<Vec<T>>,
        buf_size: usize,
    }

    impl<T> Buffer<T> {
        pub fn new(buf_size: usize) -> Self {
            Self {
                buf: <_>::default(),
                buf_size,
            }
        }
        pub fn push(&self, value: T) -> bool {
            let mut buf = self.buf.lock();
            if buf.len() >= self.buf_size {
                false
            } else {
                buf.push(value);
                true
            }
        }
        pub fn take(&self) -> Vec<T> {
            mem::take(&mut self.buf.lock())
        }
        pub fn is_empty(&self) -> bool {
            self.buf.lock().is_empty()
        }
    }
}
use buffer::Buffer;

use async_trait::async_trait;

#[async_trait]
trait Storage: Send + Sync + 'static {
    async fn init(&self) -> Result<()> {
        Ok(())
    }
    async fn save_events(&self, _events: Vec<Event>) -> Result<()> {
        unimplemented!("the engine can not save events");
    }
    async fn load_events(&self, _from: Timestamp, _to: Timestamp) -> Result<Vec<Event>> {
        unimplemented!("the engine can not load events");
    }
}

macro_rules! create_sqlx_tables {
    ($db: expr, $db_kind: ty, $time_kind: expr) => {{
        let mut qb = sqlx::QueryBuilder::<$db_kind>::new("CREATE TABLE IF NOT EXISTS events (id UUID, sensor VARCHAR(1024), value DOUBLE PRECISION, t ");
        qb.push($time_kind);
        qb.push(", PRIMARY KEY(id))");
        qb.build().execute($db).await?;
    }};
}

macro_rules! save_events {
    ($events: expr, $db: expr, $db_kind: ty) => {{
        let mut qb =
            sqlx::QueryBuilder::<$db_kind>::new("INSERT INTO events (id, sensor, value, t) ");
        qb.push_values($events, |mut b, ev| {
            b.push_bind(ev.id);
            b.push_bind(ev.sensor);
            b.push_bind(ev.value);
            b.push_bind(ev.t);
        });
        qb.build().execute($db).await?;
    }};
}

macro_rules! load_events {
    ($db: expr, $db_kind: ty, $from: expr, $to: expr) => {{
        let mut qb = sqlx::QueryBuilder::<$db_kind>::new(
            "SELECT id, sensor, value, t FROM events WHERE t BETWEEN ",
        );
        qb.push_bind($from);
        qb.push(" AND ");
        qb.push_bind($to);
        qb.build_query_as().fetch_all($db).await.map_err(Into::into)
    }};
}

#[async_trait]
impl Storage for Pool<Postgres> {
    async fn init(&self) -> Result<()> {
        create_sqlx_tables!(self, Postgres, "TIMESTAMPTZ");
        Ok(())
    }
    async fn save_events(&self, events: Vec<Event>) -> Result<()> {
        save_events!(events, self, Postgres);
        Ok(())
    }
    async fn load_events(&self, from: Timestamp, to: Timestamp) -> Result<Vec<Event>> {
        load_events!(self, Postgres, from, to)
    }
}
#[async_trait]
impl Storage for Pool<Sqlite> {
    async fn init(&self) -> Result<()> {
        create_sqlx_tables!(self, Sqlite, "INTEGER");
        Ok(())
    }
    async fn save_events(&self, events: Vec<Event>) -> Result<()> {
        save_events!(events, self, Sqlite);
        Ok(())
    }
    async fn load_events(&self, from: Timestamp, to: Timestamp) -> Result<Vec<Event>> {
        load_events!(self, Sqlite, from, to)
    }
}

mod jsonwriter {
    use super::Result;
    use serde::Serialize;
    use std::path::PathBuf;
    use tokio::{
        fs,
        io::{self, AsyncWriteExt},
    };

    pub struct JsonWriter {
        path: PathBuf,
    }

    impl JsonWriter {
        pub fn new<S>(path: S) -> Self
        where
            S: Into<PathBuf>,
        {
            Self { path: path.into() }
        }
        pub async fn write<T>(&self, data: &[T]) -> Result<()>
        where
            T: Serialize,
        {
            let fh = fs::OpenOptions::new()
                .append(true)
                .create(true)
                .truncate(false)
                .write(true)
                .open(&self.path)
                .await?;
            let mut writer = io::BufWriter::new(fh);
            for record in data {
                writer.write(&serde_json::to_vec(&record)?).await?;
                writer.write_u8(b'\n').await?;
            }
            writer.flush().await?;
            Ok(())
        }
    }
}
use jsonwriter::JsonWriter;

#[async_trait]
impl Storage for JsonWriter {
    async fn save_events(&self, events: Vec<Event>) -> Result<()> {
        self.write(&events).await
    }
}

struct Database {
    engine: Box<dyn Storage>,
    event_buf: Buffer<Event>,
    worker: Mutex<Option<JoinHandle<()>>>,
    lock: tokio::sync::Mutex<()>,
}

impl Database {
    pub async fn connect(
        db_uri: &str,
        pool_size: u32,
        timeout: Duration,
        event_buf_size: usize,
    ) -> Result<Arc<Database>> {
        macro_rules! db {
            ($pool: expr) => {
                Arc::new(Self {
                    engine: Box::new($pool),
                    event_buf: Buffer::new(event_buf_size),
                    worker: <_>::default(),
                    lock: <_>::default(),
                })
            };
        }
        let mut sp = db_uri.split("://");
        let db = match sp.next().unwrap() {
            "sqlite" => {
                let opts = sqlite::SqliteConnectOptions::from_str(db_uri)?
                    .create_if_missing(true)
                    .busy_timeout(timeout);
                let pool = sqlite::SqlitePoolOptions::new()
                    .max_connections(pool_size)
                    .acquire_timeout(timeout)
                    .connect_with(opts)
                    .await?;
                db!(pool)
            }
            "postgres" => {
                let opts = postgres::PgConnectOptions::from_str(db_uri)?;
                let pool = postgres::PgPoolOptions::new()
                    .max_connections(pool_size)
                    .acquire_timeout(timeout)
                    .connect_with(opts)
                    .await?;
                db!(pool)
            }
            "json" => {
                db!(JsonWriter::new(sp.next().expect("file name not given")))
            }
            v => unimplemented!("database kind not supported: {}", v),
        };
        // create tables, etc
        db.engine.init().await?;
        // launch database worker in the background
        db.worker.lock().replace(tokio::spawn(db.clone().worker()));
        Ok(db)
    }
    async fn worker(self: Arc<Self>) {
        let mut int = tokio::time::interval(Duration::from_secs(1));
        int.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            int.tick().await;
            let _lock = self.lock.lock().await;
            let events: Vec<Event> = self.event_buf.take();
            if !events.is_empty() {
                if let Err(e) = self.engine.save_events(events).await {
                    log::error!("unable to save: {}, events dropped", e);
                }
            }
        }
    }
    pub fn push_event(&self, event: Event) {
        if !self.event_buf.push(event) {
            log::warn!("buffer is full, event dropped");
        }
    }
    pub fn busy(&self) -> bool {
        self.lock.try_lock().is_err() || !self.event_buf.is_empty()
    }
    pub async fn load_events(&self, from: Timestamp, to: Timestamp) -> Result<Vec<Event>> {
        self.engine.load_events(from, to).await
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if let Some(fut) = self.worker.lock().take() {
            fut.abort();
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let db_uri = "sqlite://events.db";
    //let db_uri = "json://events.json";
    //let db_uri = "postgres://user:pass@dbhost/database";
    let db = Database::connect(db_uri, 1, Duration::from_secs(5), 1024).await?;
    db.push_event(Event::new("temp", 25.0));
    db.push_event(Event::new("hum", 45.0));
    while db.busy() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    dbg!(
        db.load_events(Timestamp::before24h(), Timestamp::now())
            .await?
    );
    Ok(())
}
