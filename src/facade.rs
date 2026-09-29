#![allow(clippy::too_many_arguments)]

use std::{collections::HashMap, time::Duration};

use redis::{Script, aio::ConnectionLike};

use crate::{
    Error, Result,
    helpers::{encode_metadata, make_id, map_script_err, valid_id, validate_queue_name},
    types::{Message, QueueAttributeUpdate, QueueAttributes},
};

/// Lua scripts used by the RSMQ backend for queue and message operations.
#[derive(Debug, Clone)]
pub struct RedisMqFacade {
    /// Sends a message to a queue.
    pub send_message: Script,

    /// Receives a message from a queue.
    pub receive_message: Script,

    /// Deletes a message from a queue.
    pub delete_message: Script,

    /// Removes and returns a message from a queue.
    pub pop_message: Script,

    /// Changes the visibility timeout of a message.
    pub change_visibility: Script,

    /// Deletes a queue and its associated data.
    pub delete_queue: Script,
}
impl Default for RedisMqFacade {
    fn default() -> Self {
        Self {
            send_message: Script::new(include_str!("../scripts/send_message.lua")),
            receive_message: Script::new(include_str!("../scripts/receive_message.lua")),
            delete_message: Script::new(include_str!("../scripts/delete_message.lua")),
            pop_message: Script::new(include_str!("../scripts/pop_message.lua")),
            change_visibility: Script::new(include_str!("../scripts/change_visibility.lua")),
            delete_queue: Script::new(include_str!("../scripts/delete_queue.lua")),
        }
    }
}

fn attrs(namespace: &str, q: &str) -> String {
    format!("{}:{}:Q", namespace, q)
}

fn queues_key(namespace: &str) -> String {
    format!("{}:QUEUES", namespace)
}

fn zset(namespace: &str, q: &str) -> String {
    format!("{}:{}", namespace, q)
}

impl RedisMqFacade {
    /// Creates a queue with the given visibility, delay, and size limits.
    pub async fn create_queue<C>(
        &self,
        conn: &mut C,
        namespace: &str,
        queue: &str,
        hidden: Option<Duration>,
        delay: Option<Duration>,
        maxsize: Option<i64>,
    ) -> Result<()>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        let vt = hidden.map(|d| d.as_secs()).unwrap_or(30);
        let delay = delay.map(|d| d.as_secs()).unwrap_or(0);
        let maxsize = maxsize.unwrap_or(65536);
        if vt > 9_999_999 {
            return Err(Error::InvalidValue("hidden"));
        }
        if delay > 9_999_999 {
            return Err(Error::InvalidValue("delay"));
        }
        if maxsize != -1 && !(1024..=65536).contains(&maxsize) {
            return Err(Error::InvalidValue("maxsize"));
        }

        let key = attrs(namespace, queue);

        // Time from Redis, as in the reference implementation.
        let (secs, _micros): (u64, u64) = redis::cmd("TIME").query_async(conn).await?;

        // HSETNX on the first field acts as the existence check, atomically.
        let created: bool = redis::cmd("HSETNX")
            .arg(&key)
            .arg("vt")
            .arg(vt)
            .query_async(conn)
            .await?;
        if !created {
            return Err(Error::QueueExists);
        }

        redis::pipe()
            .atomic()
            .cmd("HSET")
            .arg(&key)
            .arg("delay")
            .arg(delay)
            .arg("maxsize")
            .arg(maxsize)
            .arg("created")
            .arg(secs)
            .arg("modified")
            .arg(secs)
            .arg("totalrecv")
            .arg(0)
            .arg("totalsent")
            .arg(0)
            .ignore()
            .cmd("SADD")
            .arg(queues_key(namespace))
            .arg(queue)
            .ignore()
            .query_async::<()>(conn)
            .await?;
        Ok(())
    }

    /// Sends a message to a queue and returns its generated message ID.
    pub async fn send_message<C>(
        &self,
        conn: &mut C,
        namespace: &str,
        queue: &str,
        message: Vec<u8>,
        metadata: HashMap<String, String>,
        realtime: bool,
        delay: Option<Duration>,
    ) -> Result<String>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        let (secs, micros): (u64, u64) = redis::cmd("TIME").query_async(conn).await?;
        let id = make_id(secs, micros);

        let meta = encode_metadata(&metadata)?;
        // -1 => use the queue's default delay (resolved inside the script).
        let delay_ms: i64 = delay.map(|d| d.as_millis() as i64).unwrap_or(-1);

        let res: std::result::Result<String, redis::RedisError> = self
            .send_message
            .key(zset(namespace, queue))
            .key(attrs(namespace, queue))
            .arg(&id)
            .arg(message)
            .arg(delay_ms)
            .arg(meta)
            .arg(if realtime { "1" } else { "0" })
            .arg(queue)
            .arg(namespace)
            .invoke_async(conn)
            .await;

        map_script_err(res)
    }
    /// Receives a message from a queue, optionally hiding it for a duration.
    pub async fn receive_message<C>(
        &self,
        conn: &mut C,
        namespace: &str,
        queue: &str,
        hidden: Option<Duration>,
    ) -> Result<Option<Message>>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        let hidden_ms: i64 = hidden.map(|d| d.as_millis() as i64).unwrap_or(-1);

        type Raw = Vec<redis::Value>;
        let res: std::result::Result<Raw, redis::RedisError> = self
            .receive_message
            .key(zset(namespace, queue))
            .key(attrs(namespace, queue))
            .arg(hidden_ms)
            .invoke_async(conn)
            .await;
        let raw = map_script_err(res)?;
        if raw.is_empty() {
            return Ok(None);
        }
        // raw = [id, msg, rc, fr, meta]
        let id: String = redis::from_redis_value_ref(&raw[0])?;
        let message: Vec<u8> = redis::from_redis_value_ref(&raw[1])?;
        let attempt = redis::from_redis_value_ref(&raw[2])?;
        let received_at = redis::from_redis_value_ref(&raw[3])?;
        let meta: Vec<u8> = redis::from_redis_value_ref(&raw[4])?;
        let metadata = serde_json::from_slice(&meta).unwrap_or_default();
        Ok(Some(Message {
            id,
            message,
            metadata,
            attempt,
            received_at,
        }))
    }
    /// Deletes a message from a queue.
    pub async fn delete_message<C>(
        &self,
        conn: &mut C,
        namespace: &str,
        queue: &str,
        id: &str,
    ) -> Result<bool>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        if !valid_id(id) {
            return Err(Error::InvalidFormat);
        }
        let res: std::result::Result<i64, redis::RedisError> = self
            .delete_message
            .key(zset(namespace, queue))
            .key(attrs(namespace, queue))
            .arg(id)
            .invoke_async(conn)
            .await;
        Ok(map_script_err(res)? == 1)
    }

    /// Deletes a queue and all of its associated messages.
    pub async fn delete_queue<C>(&self, conn: &mut C, namespace: &str, queue: &str) -> Result<()>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        let res: std::result::Result<i64, redis::RedisError> = self
            .delete_queue
            .key(zset(namespace, queue))
            .key(attrs(namespace, queue))
            .key(queues_key(namespace))
            .arg(queue)
            .invoke_async(conn)
            .await;
        map_script_err(res)?;
        Ok(())
    }

    /// Lists all queues in the namespace.
    pub async fn list_queues<C>(&self, conn: &mut C, namespace: &str) -> Result<Vec<String>>
    where
        C: ConnectionLike,
    {
        let mut queues: Vec<String> = redis::cmd("SMEMBERS")
            .arg(queues_key(namespace))
            .query_async(conn)
            .await?;
        queues.sort(); // SMEMBERS order is unspecified; sort for stable output
        Ok(queues)
    }

    /// Returns the attributes of a queue.
    pub async fn get_queue_attributes<C>(
        &self,
        conn: &mut C,
        namespace: &str,
        queue: &str,
    ) -> Result<QueueAttributes>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        let (secs, micros): (u64, u64) = redis::cmd("TIME").query_async(conn).await?;
        let now_ms = secs * 1000 + micros / 1000;

        // One MULTI/EXEC so the counts and attributes are a consistent snapshot.
        let (vals, msgs, hidden): (Vec<Option<String>>, u64, u64) = redis::pipe()
            .atomic()
            .cmd("HMGET")
            .arg(attrs(namespace, queue))
            .arg(&[
                "vt",
                "delay",
                "maxsize",
                "totalrecv",
                "totalsent",
                "created",
                "modified",
            ])
            .cmd("ZCARD")
            .arg(zset(namespace, queue))
            .cmd("ZCOUNT")
            .arg(zset(namespace, queue))
            .arg(now_ms)
            .arg("+inf")
            .query_async(conn)
            .await?;

        if vals.iter().any(|v| v.is_none()) {
            return Err(Error::QueueNotFound);
        }
        let n = |i: usize| -> Result<i64> {
            vals[i]
                .as_deref()
                .unwrap()
                .parse::<i64>()
                .map_err(|_| Error::InvalidValue("corrupt queue attribute"))
        };
        Ok(QueueAttributes {
            vt: Duration::from_secs(n(0)? as u64),
            delay: Duration::from_secs(n(1)? as u64),
            maxsize: n(2)?,
            totalrecv: n(3)? as u64,
            totalsent: n(4)? as u64,
            created: n(5)? as u64,
            modified: n(6)? as u64,
            msgs,
            hiddenmsgs: hidden,
        })
    }

    /// Updates the attributes of a queue.
    pub async fn set_queue_attributes<C>(
        &self,
        conn: &mut C,
        namespace: &str,
        queue: &str,
        update: QueueAttributeUpdate,
    ) -> Result<QueueAttributes>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        if update.hidden.is_none() && update.delay.is_none() && update.maxsize.is_none() {
            return Err(Error::InvalidValue("no attribute supplied"));
        }
        if let Some(h) = update.hidden
            && h.as_secs() > 9_999_999
        {
            return Err(Error::InvalidValue("hidden"));
        }
        if let Some(d) = update.delay
            && d.as_secs() > 9_999_999
        {
            return Err(Error::InvalidValue("delay"));
        }
        if let Some(m) = update.maxsize
            && m != -1
            && !(1024..=65536).contains(&m)
        {
            return Err(Error::InvalidValue("maxsize"));
        }

        let key = attrs(namespace, queue);
        let exists: bool = redis::cmd("EXISTS").arg(&key).query_async(conn).await?;
        if !exists {
            return Err(Error::QueueNotFound);
        }

        let (secs, _): (u64, u64) = redis::cmd("TIME").query_async(conn).await?;
        let mut c = redis::cmd("HSET");
        c.arg(&key).arg("modified").arg(secs);
        if let Some(h) = update.hidden {
            c.arg("vt").arg(h.as_secs());
        }
        if let Some(d) = update.delay {
            c.arg("delay").arg(d.as_secs());
        }
        if let Some(m) = update.maxsize {
            c.arg("maxsize").arg(m);
        }
        c.query_async::<()>(conn).await?;

        self.get_queue_attributes(conn, namespace, queue).await
    }

    /// Changes the visibility timeout of a message.
    pub async fn change_message_visibility<C>(
        &self,
        conn: &mut C,
        namespace: &str,
        queue: &str,
        id: &str,
        hidden: Duration,
    ) -> Result<bool>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        if !valid_id(id) {
            return Err(Error::InvalidFormat);
        }
        if hidden.as_secs() > 9_999_999 {
            return Err(Error::InvalidValue("hidden"));
        }

        let res: std::result::Result<i64, redis::RedisError> = self
            .change_visibility
            .key(zset(namespace, queue))
            .key(attrs(namespace, queue))
            .arg(id)
            .arg(hidden.as_millis() as i64)
            .invoke_async(conn)
            .await;
        Ok(map_script_err(res)? == 1)
    }

    /// Removes and returns a message from a queue.
    pub async fn pop_message<C>(
        &self,
        conn: &mut C,
        namespace: &str,
        queue: &str,
    ) -> Result<Option<Message>>
    where
        C: ConnectionLike,
    {
        validate_queue_name(queue)?;
        type Raw = Vec<redis::Value>;
        let res: std::result::Result<Raw, redis::RedisError> = self
            .pop_message
            .key(zset(namespace, queue))
            .key(attrs(namespace, queue))
            .invoke_async(conn)
            .await;
        let raw = map_script_err(res)?;
        if raw.is_empty() {
            return Ok(None);
        }
        // raw = [id, msg, rc, fr, meta]
        let id: String = redis::from_redis_value_ref(&raw[0])?;
        let message: Vec<u8> = redis::from_redis_value_ref(&raw[1])?;
        let attempt = redis::from_redis_value_ref(&raw[2])?;
        let received_at = redis::from_redis_value_ref(&raw[3])?;
        let meta: Vec<u8> = redis::from_redis_value_ref(&raw[4])?;
        let metadata = serde_json::from_slice(&meta).unwrap_or_default();
        Ok(Some(Message {
            id,
            message,
            metadata,
            attempt,
            received_at,
        }))
    }
}
