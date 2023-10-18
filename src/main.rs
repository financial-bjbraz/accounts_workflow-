use ferris_says::say;
use std::io::{stdout, BufWriter};
use deadpool_lapin::{Manager, Pool, PoolError};
use futures::{join, StreamExt};
use lapin::{options::*, types::FieldTable, BasicProperties, ConnectionProperties};
use std::convert::Infallible;
use std::result::Result as StdResult;
use std::time::Duration;
use thiserror::Error as ThisError;
use tokio_amqp::*;
use warp::{Filter, Rejection, Reply};
type WebResult<T> = StdResult<T, Rejection>;
type RMQResult<T> = StdResult<T, PoolError>;
type Result<T> = StdResult<T, Error>;
type Connection = deadpool::managed::Object<deadpool_lapin::Manager>;

use lapin::{
    options::*, publisher_confirm::Confirmation
};

#[path = "service/account.rs"]
mod account;

#[path = "database/mongo.rs"]
mod database;

#[path = "queue/publisher.rs"]
mod publisher;

#[derive(ThisError, Debug)]
enum Error {
    #[error("rmq error: {0}")]
    RMQError(#[from] lapin::Error),
    #[error("rmq pool error: {0}")]
    RMQPoolError(#[from] PoolError),
}

impl warp::reject::Reject for Error {}

fn with_rmq(pool: Pool) -> impl Filter<Extract = (Pool,), Error = Infallible> + Clone {
    warp::any().map(move || pool.clone())
}

async fn add_msg_handler(pool: Pool) -> WebResult<impl Reply> {

    let rmq_con = get_rmq_con(pool).await.map_err(|e| {
        eprintln!("can't connect to rmq, {}", e);
        warp::reject::custom(Error::RMQPoolError(e))
    })?;

    let channel = rmq_con.create_channel().await.map_err(|e| {
        eprintln!("can't create channel, {}", e);
        warp::reject::custom(Error::RMQError(e))
    })?;

    let payload = r#"
    {
        "idempotencyKey": "03fd15c3-0537-4f5a-9a48-5d4278303dc6",
        "dt_ref": 1234,
        "accountNumber":1,
        "owner": {
            "name": "Ebano de Oliveira",
            "surName": "jr",
          "personType": "PF",
          "birthDate": "1981-10-10",
          "companyName": "ALEX-CHILD-TEST",
          "email": "contato@bjbraz.com.br",
          "homePhone": "1131859600",
          "businessPhone": "1131859600",
          "mobilePhone": "1131859600",
          "address": {
            "street": "Rua dos Testes",
            "number": "123",
            "district": "Tamboré",
            "zipcode": "06460080",
            "city": "Barueri",
            "state": "SP",
            "complement": ""
          },
          "identifierDocument": {
            "document": "23945231078",
            "documentType": "CPF"
          },
          "documents": [
            {
              "document": "23945231078",
              "documentType": "CPF"
            }
          ]
        },
        "responsible": {
          "personType": "PF",
          "name": "Ebano de Oliveira",
          "surName": "jR",
          "email": "contato@bjbraz.com.br",
          "homePhone": "1131859600",
          "businessPhone": "1131859600",
          "mobilePhone": "1131859600",
          "birthDate": "1982-02-02",
          "companyName":"Companie",
          "address": {
            "street": "Rua dos Testes",
            "number": "123",
            "district": "Tamboré",
            "zipcode": "06460080",
            "city": "Barueri",
            "state": "SP",
            "complement":""
          },
          "identifierDocument": {
            "document": "23945231078",
            "documentType": "CPF"
          },
          "documents": [
            {
              "document": "23945231078",
              "documentType": "CPF"
            }
          ]
        }
      }"#;

    channel
        .basic_publish(
            "",
            "hello",
            BasicPublishOptions::default(),
            payload.as_bytes().to_vec(),
            BasicProperties::default(),
        )
        .await
        .map_err(|e| {
            eprintln!("can't publish: {}", e);
            warp::reject::custom(Error::RMQError(e))
        })?
        .await
        .map_err(|e| {
            eprintln!("can't publish: {}", e);
            warp::reject::custom(Error::RMQError(e))
        })?;
    Ok("OK")
}

async fn health_handler() -> WebResult<impl Reply> {
    Ok("OK")
}

async fn get_rmq_con(pool: Pool) -> RMQResult<Connection> {
    let connection = pool.get().await?;
    Ok(connection)
}

async fn rmq_listen(pool: Pool) -> Result<()> {
    let mut retry_interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        retry_interval.tick().await;
        println!("connecting rmq consumer...");
        match init_rmq_listen(pool.clone()).await {
            Ok(_) => println!("rmq listen returned"),
            Err(e) => eprintln!("rmq listen had an error: {}", e),
        };
    }
}

async fn init_rmq_listen(pool: Pool) -> Result<()> {
    let rmq_con = get_rmq_con(pool).await.map_err(|e| {
        eprintln!("could not get rmq con: {}", e);
        e
    })?;
    let channel = rmq_con.create_channel().await?;

    let queue = channel
        .queue_declare(
            "hello",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;
    println!("Declared queue {:?}", queue);

    let mut consumer = channel
        .basic_consume(
            "hello",
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("rmq consumer connected, waiting for messages");
    while let Some(delivery) = consumer.next().await {
        if let Ok((channel, delivery)) = delivery {
            let s = String::from_utf8_lossy(&delivery.data);
            println!("received msg: {:?} {:?}", s, delivery);

            let account_object = account::convert_to_object(s.to_string()).unwrap();

            let message = account::Message {
                idempotency_key: account_object.idempotency_key,
                account_number: account_object.account_number,
                owner: account_object.owner,
                responsible: account_object.responsible,
                account_status: Some(String::from("BUREAU_VALIDATION")),
            };

            let valor = database::insert_on_database(message).await; //?

            let _is_ok = valor.map_err(|err| println!("{:?}", err)).ok();

            channel.basic_ack(delivery.delivery_tag, BasicAckOptions::default())
            .await?;

            let account_object2 = account::convert_to_object(s.to_string()).unwrap();
            let message_copied = account::Message {
                idempotency_key: account_object2.idempotency_key,
                account_number: account_object2.account_number,
                owner: account_object2.owner,
                responsible: account_object2.responsible,
                account_status: Some(String::from("BUREAU_VALIDATION")),
            };

            let message_string = account::deserializer(message_copied).unwrap();

            // let publish = channel
            // .basic_publish(
            //     "",
            //     "accounts_bureau_validate",
            //     BasicPublishOptions::default(),
            //     message_string.unwrap().as_bytes().to_vec(),
            //     BasicProperties::default(),
            // );

            // publish.await?.await?;

        //     async_global_executor::block_on(async {
        //     loop {
        //         let confirm = channel
        //             .basic_publish(
        //                 "",
        //                 "hello",
        //                 BasicPublishOptions::default(),
        //                 message_string.as_bytes().to_vec(),
        //                 BasicProperties::default(),
        //             )
        //             .await?
        //             .await?;
        //         assert_eq!(confirm, Confirmation::NotRequested);
        //     }
        // })
        


        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {

    let stdout = stdout();
    let message = String::from("Hello fellow Rustaceans!");
    let width = message.chars().count();

    let mut writer = BufWriter::new(stdout.lock());
    say(message.as_bytes(), width, &mut writer).unwrap();

    let addr =
        std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://guest:guest@192.168.0.18:5672/%2f".into());
    let manager = Manager::new(addr, ConnectionProperties::default().with_tokio());
    let pool: Pool = deadpool::managed::Pool::builder(manager)
        .max_size(100)
        .build()
        .expect("can create pool");

    let health_route = warp::path!("health").and_then(health_handler);
    let add_msg_route = warp::path!("msg")
        .and(warp::post())
        .and(with_rmq(pool.clone()))
        .and_then(add_msg_handler);
    let routes = health_route.or(add_msg_route);

    println!("Started server at localhost:8093");
    let _ = join!(
        warp::serve(routes).run(([0, 0, 0, 0], 8093)),
        rmq_listen(pool.clone())
    );

    Ok(())

}