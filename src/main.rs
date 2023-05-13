use std::{
    env,
    error::Error,
    ffi::OsString,
    fs::File,
    process,
    sync::{Arc, Mutex},
};

use csv::Reader;
use postgres::Client;

fn main() {
    println!("Hello, world!");

    let db = connect_database();
    let database_connection = match db {
        Err(e) => panic!("{}", e),
        Ok(c) => c,
    };
    let client: Arc<Mutex<Client>> = Arc::new(Mutex::new(database_connection));

    drop_tables(&client);
    create_tables(&client);

    let _database_connection = match run_csv(&client) {
        Err(e) => panic!("{}", e),
        Ok(c) => c,
    };
}

fn connect_database() -> Result<Client, postgres::Error> {
    let client: Client = Client::connect(
        "postgresql://brian:1234@localhost:5432/locationdb",
        postgres::NoTls,
    )?;

    Ok(client)
}

fn drop_tables(client: &Arc<Mutex<Client>>) {
    let client = Arc::clone(client);
    let mut db_connection = client.lock().unwrap();

    if let Err(err) = db_connection.batch_execute("drop table if exists cities") {
        println!("{}", err);
        process::exit(1);
    };

    if let Err(err) = db_connection.batch_execute("drop table if exists states") {
        println!("{}", err);
        process::exit(1);
    };

    if let Err(err) = db_connection.batch_execute("drop table if exists countries") {
        println!("{}", err);
        process::exit(1);
    };
}

fn create_tables(client: &Arc<Mutex<Client>>) {
    let client = Arc::clone(client);
    let mut db_connection = client.lock().unwrap();

    if let Err(err) = db_connection.batch_execute(
        "
    create table countries(
        id    SERIAL PRIMARY KEY NOT NULL,
        name    varchar(100) NOT NULL ,
        iso3 char(3)  DEFAULT NULL,
      numeric_code char(3)  DEFAULT NULL,
      iso2 char(2)  DEFAULT NULL,
      phonecode varchar(255)  DEFAULT NULL,
      capital varchar(255)  DEFAULT NULL,
      currency varchar(255)  DEFAULT NULL,
      currency_name varchar(255)  DEFAULT NULL,
      currency_symbol varchar(255)  DEFAULT NULL,
      tld varchar(255)  DEFAULT NULL,
      native varchar(255)  DEFAULT NULL,
      region varchar(255)  DEFAULT NULL,
      subregion varchar(255)  DEFAULT NULL,
      timezones text ,
      created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    ",
    ) {
        println!("{}", err);
        process::exit(1);
    };

    if let Err(err) = db_connection.batch_execute(
        "
    create table states(
        id    SERIAL PRIMARY KEY NOT NULL,
        name varchar(255)  NOT NULL,
     country_id int NOT NULL REFERENCES countries(id),
     country_code char(2)  NOT NULL,
     created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP ,
     updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP 
   )
    ",
    ) {
        println!("{}", err);
        process::exit(1);
    };

    if let Err(err) = db_connection.batch_execute(
        "
    create table cities(
        id    SERIAL PRIMARY KEY NOT NULL,
         name varchar(255)  NOT NULL,
     state_id int  NOT NULL REFERENCES states(id),
     state_code varchar(255)  NOT NULL,
     country_id int  NOT NULL REFERENCES countries(id),
     country_code char(2)  NOT NULL,
     created_at timestamp NOT NULL DEFAULT '2014-01-01 06:31:01',
     updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP 
   )",
    ) {
        println!("{}", err);
        process::exit(1);
    };
}

#[derive(Debug, serde::Deserialize)]
struct Country {
    id: i32,
    name: String,
    iso3: String,
    numeric_code: String,
    iso2: String,
    phone_code: String,
    capital: String,
    currency: String,
    currency_name: String,
    currency_symbol: String,
    tld: String,
    native: String,
    region: String,
    subregion: String,
    timezones: String,
}

fn add_countries(client: &Arc<Mutex<Client>>, mut rdr: Reader<File>) -> Result<(), Box<dyn Error>> {
    let client = Arc::clone(client);
    let mut db_connection = client.lock().unwrap();
    for result in rdr.deserialize() {
        let record: Country = result?;
            println!("Country id : {:?}", record.id);

            if let Err(err) = db_connection.execute(
                "INSERT INTO countries (id,name,iso3,numeric_code,iso2,phonecode,capital,currency,
                    currency_name,currency_symbol,tld,native,region,subregion,timezones) 
                    VALUES ($1, $2, $3 , $4, $5 , $6, $7, $8,$9,$10,$11,$12,$13,$14,$15)",
                &[
                    &record.id,
                    &record.name,
                    &record.iso3,
                    &record.numeric_code,
                    &record.iso2,
                    &record.phone_code,
                    &record.capital,
                    &record.currency,
                    &record.currency_name,
                    &record.currency_symbol,
                    &record.tld,
                    &record.native,
                    &record.region,
                    &record.subregion,
                    &record.timezones,
                ],
            ) {
                println!("{}", err);
                process::exit(1);
            }
    }



    Ok(())
}

#[derive(Debug, serde::Deserialize)]

struct State {
    id: i32,
    name: String,
    country_id: i32,
    country_code: String,
}

fn add_states(client: &Arc<Mutex<Client>>, mut rdr: Reader<File>) -> Result<(), Box<dyn Error>> {
    let client = Arc::clone(client);
    let mut db_connection = client.lock().unwrap();
    for result in rdr.deserialize() {
        let record: State = result?;
            println!("State Id : {:?}", record.id);

            if let Err(err) = db_connection.execute(
                "INSERT INTO states (id,name,country_id,country_code) 
                    VALUES ($1, $2, $3 , $4)",
                &[
                    &record.id,
                    &record.name,
                    &record.country_id,
                    &record.country_code,
                ],
            ) {
                println!("{}", err);
                process::exit(1);
            }
    }



    Ok(())
}


#[derive(Debug, serde::Deserialize)]

struct City {
    id: i32,
    name: String,
    country_id: i32,
    country_code: String,
    state_id: i32,
    state_code: String,
}

fn add_cities(client: &Arc<Mutex<Client>>, mut rdr: Reader<File>) -> Result<(), Box<dyn Error>> {
    let client = Arc::clone(client);
    let mut db_connection = client.lock().unwrap();
    for result in rdr.deserialize() {
        let record: City = result?;
            println!("City Id : {:?}", record.id);

            if let Err(err) = db_connection.execute(
                "INSERT INTO states (id,name,country_id,country_code,state_id,state_code) 
                    VALUES ($1, $2, $3 , $4,$5,$6)",
                &[
                    &record.id,
                    &record.name,
                    &record.country_id,
                    &record.country_code,
                    &record.state_id,
                    &record.state_code,
                ],
            ) {
                println!("{}", err);
                process::exit(1);
            }
    }


    Ok(())
}

fn run_csv(client: &Arc<Mutex<Client>>) -> Result<(), Box<dyn Error>> {

    {
        let file_path = get_arg(1)?;
        let file = File::open(file_path)?;
        let country_rdr = csv::Reader::from_reader(file);

        // country
        // add_country(client, rdr)
        match add_countries(client, country_rdr) {
            Err(e) => println!("{:?}", e),
            _ => ()
        }
    }
    {
        let file_path = get_arg(2)?;
        let file = File::open(file_path)?;
        let state_rdr = csv::Reader::from_reader(file);

        // country
        // add_country(client, rdr)
        match add_states(client, state_rdr) {
            Err(e) => println!("{:?}", e),
            _ => () 
        }
    }
    {
        let file_path = get_arg(3)?;
        let file = File::open(file_path)?;
        let city_rdr = csv::Reader::from_reader(file);

        // country
        // add_country(client, rdr)
        match add_cities(client, city_rdr) {
            Err(e) => println!("{:?}", e),
            _ => () 
        }
    }
    Ok(())
}

fn get_arg(n: usize) -> Result<OsString, Box<dyn Error>> {
    match env::args_os().nth(n) {
        None => Err(From::from("expected 1 argument, but got none")),
        Some(file_path) => Ok(file_path),
    }
}
