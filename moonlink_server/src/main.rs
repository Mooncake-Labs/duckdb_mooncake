use tokio_postgres::{connect, NoTls};
use tokio::net::UnixStream;

#[tokio::main]
pub async fn main() {
    let uri = "postgresql://vscode@localhost:5432/vscode";
    let (client, connection) = connect(uri, NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("{e:?}");
        }
    });
    client.simple_query("DROP SCHEMA IF EXISTS mooncake CASCADE").await.unwrap();
    client.simple_query("CREATE SCHEMA mooncake").await.unwrap();
    client.simple_query("DROP TABLE IF EXISTS r").await.unwrap();
    client.simple_query("CREATE TABLE r (a int, b text, c real, d int)").await.unwrap();
    client.simple_query("INSERT INTO r VALUES (1, 'a', 1.2, 6), (2, 'b', 3.4, 7), (3, 'c', 5.6, 8)").await.unwrap();

    tokio::spawn(async {
        let config = moonlink_service::ServiceConfig {
            base_path: "/home/vscode/pgdata/pg_mooncake".to_owned(),
            data_server_uri: None,
            rest_api_port: None,
            tcp_port: None,
        };
        if let Err(e) = moonlink_service::start_with_config(config).await {
            eprintln!("{e:?}");
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let mut stream = UnixStream::connect("/home/vscode/pgdata/pg_mooncake/moonlink.sock").await.unwrap();
    moonlink_rpc::create_table(&mut stream, "abc".to_owned(), "def".to_owned(), "public.r".to_owned(), uri.to_owned(), "{}".to_owned()).await.unwrap();
    let data = moonlink_rpc::get_table_schema(&mut stream, "abc".to_owned(), "def".to_owned()).await.unwrap();
    println!("{data:?}");

    loop {}
}
