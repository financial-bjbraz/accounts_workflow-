use ferris_says::say;
use std::io::{stdout, BufWriter};
use deadpool_lapin::{Manager, Pool, PoolError};
use futures::{join, StreamExt};
use lapin::{options::*, types::FieldTable, BasicProperties, ConnectionProperties};
use std::convert::Infallible;
use std::result::Result as StdResult;
use tokio_amqp::*;
type RMQResult<T> = StdResult<T, PoolError>;
type Connection = deadpool::managed::Object<deadpool_lapin::Manager>;

use std::error::Error;

use lapin::{
    options::*, publisher_confirm::Confirmation
};

async fn get_rmq_con(pool: Pool) -> RMQResult<Connection> {
    let connection = pool.get().await?;
    Ok(connection)
}

pub async fn add_msg_handler(payload: String, queue: String) -> Result<()>  {

    let addr =
        std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://guest:guest@192.168.0.18:5672/%2f".into());
    let manager = Manager::new(addr, ConnectionProperties::default().with_tokio());

    let pool: Pool = deadpool::managed::Pool::builder(manager)
        .max_size(100)
        .build()
        .expect("can create pool");

    // let rmq_con = get_rmq_con(pool).await.map_err(|e| {
    //     eprintln!("can't connect to rmq, {}", e);
    // })?;

    // let channel = rmq_con.create_channel().await.map_err(|e| {
    //     eprintln!("can't create channel, {}", e);
    // })?;

    let rmq_con = get_rmq_con(pool).await.map_err(|e| {
        eprintln!("can't connect to rmq, {}", e);
        Ok(())
    }).unwrap();

    let channel = rmq_con.create_channel().await.map_err(|e| {
        eprintln!("can't create channel, {}", e);
        Ok(())
    }).unwrap();
 
    channel
        .basic_publish(
            "",
            &queue,
            BasicPublishOptions::default(),
            payload.as_bytes().to_vec(),
            BasicProperties::default(),
        )
        .await
        .map_err(|err| println!("{:?}", err));

        Ok(())
 }