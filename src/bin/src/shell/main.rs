// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::HashMap, io::Write, time::Duration};

use clap::Parser;
use engula_client::{AppError, ClientOptions, Collection, Database, EngulaClient, Partition};
use lazy_static::lazy_static;
use rustyline::{error::ReadlineError, Editor};

type ParseResult<T = Request> = std::result::Result<T, String>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Parser)]
#[clap(about = "Start engula shell")]
pub struct Command {
    /// Sets the address of the target cluster to operate
    #[clap(long, default_value = "0.0.0.0:21805")]
    addrs: Vec<String>,
}

impl Command {
    pub fn run(self) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            editor_main(self.addrs).await;
        });
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Token {
    Get,
    Put,
    Delete,
    Help,
    Config,
    Db,
    Coll,
}

enum Request {
    None,
    Usage,
    Get {
        key: Vec<u8>,
        db: String,
        coll: String,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        db: String,
        coll: String,
    },
    Delete {
        key: Vec<u8>,
        db: String,
        coll: String,
    },
    Config {
        key: String,
        value: String,
    },
}

struct Session {
    client: EngulaClient,
    config: HashMap<String, String>,
    databases: HashMap<String, Database>,
    collections: HashMap<String, Collection>,
}

const CONFIG_DB: &str = "database";
const CONFIG_COLL: &str = "collection";
const CONFIG_CREATE_IF_MISSING: &str = "create-if-missing";

impl Session {
    async fn parse_and_execute(&mut self, input: &[u8]) -> Result<()> {
        match self.parse_request(input)? {
            Request::None => Ok(()),
            Request::Usage => {
                usage()?;
                Ok(())
            }
            Request::Config { key, value } => {
                self.config.insert(key, value);
                Ok(())
            }
            Request::Get { key, db, coll } => {
                let db = self.open_database(&db).await?;
                let coll = self.open_collection(&db, &coll).await?;
                if let Some(value) = coll.get(key).await? {
                    std::io::stdout().write_all(&value)?;
                }
                std::io::stdout().write_all(&[b'\n'])?;
                std::io::stdout().flush()?;
                Ok(())
            }
            Request::Put {
                key,
                value,
                db,
                coll,
            } => {
                let db = self.open_database(&db).await?;
                let coll = self.open_collection(&db, &coll).await?;
                coll.put(key, value).await?;
                Ok(())
            }
            Request::Delete { key, db, coll } => {
                let db = self.open_database(&db).await?;
                let coll = self.open_collection(&db, &coll).await?;
                coll.delete(key).await?;
                Ok(())
            }
        }
    }

    fn parse_request(&self, input: &[u8]) -> ParseResult {
        let input = skip_space(input);
        match next_token(input) {
            Some((input, Token::Get)) => self.parse_get_request(input),
            Some((input, Token::Put)) => self.parse_put_request(input),
            Some((input, Token::Delete)) => self.parse_delete_request(input),
            Some((input, Token::Config)) => self.parse_config_request(input),
            Some((_, Token::Help)) => Ok(Request::Usage),
            _ => {
                if is_eof(input) {
                    Ok(Request::None)
                } else if let Ok(input) = String::from_utf8(input.to_owned()) {
                    Err(format!("unknown sequences: {input}",))
                } else {
                    Err(format!("unknown sequences: {input:?}",))
                }
            }
        }
    }

    fn parse_get_request(&self, input: &[u8]) -> ParseResult {
        let input = skip_space(input);
        let Some((input, key)) = read_entry(input) else {
            return Err("expect key, but nothing are found".to_owned());
        };

        let (input, db) = self.parse_or_get_config(input, Token::Db, CONFIG_DB)?;
        let (input, coll) = self.parse_or_get_config(input, Token::Coll, CONFIG_COLL)?;

        must_eof(input)?;

        Ok(Request::Get { key, db, coll })
    }

    fn parse_put_request(&self, input: &[u8]) -> ParseResult {
        let input = skip_space(input);
        let Some((input, key)) = read_entry(input) else {
            return Err("expect key, but nothing are found".to_owned());
        };

        let input = skip_space(input);
        let Some((input, value)) = read_entry(input) else {
            return Err("expect value, but nothing are found".to_owned());
        };

        let (input, db) = self.parse_or_get_config(input, Token::Db, CONFIG_DB)?;
        let (input, coll) = self.parse_or_get_config(input, Token::Coll, CONFIG_COLL)?;

        must_eof(input)?;

        Ok(Request::Put {
            key,
            value,
            db,
            coll,
        })
    }

    fn parse_delete_request(&self, input: &[u8]) -> ParseResult {
        let input = skip_space(input);
        let Some((input, key)) = read_entry(input) else {
            return Err("expect key, but nothing are found".to_owned());
        };

        let (input, db) = self.parse_or_get_config(input, Token::Db, CONFIG_DB)?;
        let (input, coll) = self.parse_or_get_config(input, Token::Coll, CONFIG_COLL)?;

        must_eof(input)?;

        Ok(Request::Delete { key, db, coll })
    }

    fn parse_config_request(&self, input: &[u8]) -> ParseResult {
        let input = skip_space(input);
        let Some((input, key)) = read_entry(input) else {
            return Err("expect key, but nothing are found".to_owned());
        };
        let key =
            String::from_utf8(key).map_err(|_| "the key is invalid UTF-8 sequence".to_string())?;

        let input = skip_space(input);
        let Some((input, value)) = read_entry(input) else {
            return Err("expect value, but nothing are found".to_owned());
        };

        let value = String::from_utf8(value)
            .map_err(|_| "the key is invalid UTF-8 sequence".to_string())?;
        must_eof(input)?;

        Ok(Request::Config { key, value })
    }

    fn parse_or_get_config<'a>(
        &self,
        input: &'a [u8],
        tok: Token,
        config: &str,
    ) -> ParseResult<(&'a [u8], String)> {
        if let Some((next, item)) = self.parse_config_item(input, tok, config)? {
            Ok((next, item))
        } else if let Some(item) = self.config.get(config) {
            Ok((input, item.to_owned()))
        } else {
            Err(format!("no {config} specified"))
        }
    }

    fn parse_config_item<'a>(
        &self,
        input: &'a [u8],
        tok: Token,
        name: &str,
    ) -> ParseResult<Option<(&'a [u8], String)>> {
        let input = skip_space(input);
        if let Some(input) = expect_token(input, tok) {
            let input = skip_space(input);
            let Some((input, item)) = read_entry(input) else {
                return Err(format!("expect {name}, but nothing are found"));
            };
            let item = String::from_utf8(item)
                .map_err(|_| format!("the value of {name} is invalid UTF-8 sequence"))?;
            return Ok(Some((input, item)));
        }
        Ok(None)
    }

    async fn create_or_open_database(&mut self, database: &str) -> Result<Database> {
        match self.client.create_database(database.to_owned()).await {
            Ok(db) => {
                self.databases.insert(database.to_owned(), db.clone());
                Ok(db)
            }
            Err(AppError::AlreadyExists(_)) => {
                Ok(self.client.open_database(database.to_owned()).await?)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn create_or_open_collection(
        &mut self,
        db: &Database,
        collection: &str,
        num_shards: u32,
    ) -> Result<Collection> {
        let partition = Partition::Hash { slots: num_shards };
        match db
            .create_collection(collection.to_owned(), Some(partition))
            .await
        {
            Ok(co) => {
                self.collections
                    .insert(format!("{}-{}", db.name(), collection), co.clone());
                Ok(co)
            }
            Err(AppError::AlreadyExists(_)) => {
                Ok(db.open_collection(collection.to_owned()).await?)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn open_database(&mut self, name: &str) -> Result<Database> {
        let create_if_missing = self
            .config
            .get(CONFIG_CREATE_IF_MISSING)
            .map(|v| v == "true")
            .unwrap_or_default();

        if let Some(db) = self.databases.get(name) {
            Ok(db.clone())
        } else {
            match self.client.open_database(name.to_owned()).await {
                Ok(db) => {
                    self.databases.insert(name.to_owned(), db.clone());
                    Ok(db)
                }
                Err(AppError::NotFound(_)) if create_if_missing => {
                    Ok(self.create_or_open_database(name).await?)
                }
                Err(e) => Err(e.into()),
            }
        }
    }

    async fn open_collection(&mut self, db: &Database, coll: &str) -> Result<Collection> {
        let create_if_missing = self
            .config
            .get(CONFIG_CREATE_IF_MISSING)
            .map(|v| v == "true")
            .unwrap_or_default();

        let name = format!("{}-{}", db.name(), coll);
        if let Some(co) = self.collections.get(&name).cloned() {
            Ok(co)
        } else {
            let co = match db.open_collection(coll.to_owned()).await {
                Ok(co) => {
                    self.collections.insert(coll.to_owned(), co.clone());
                    co
                }
                Err(AppError::NotFound(_)) if create_if_missing => {
                    self.create_or_open_collection(db, coll, 64).await?
                }
                Err(e) => {
                    return Err(e.into());
                }
            };
            Ok(co)
        }
    }
}

async fn editor_main(addrs: Vec<String>) {
    let mut session = new_session(addrs).await.expect("new session");
    let mut editor = Editor::<()>::new().expect("Editor::new");
    loop {
        let readline = editor.readline(">> ");
        match readline {
            Ok(line) => {
                editor.add_history_entry(line.as_str());
                if let Err(err) = session.parse_and_execute(line.as_str().as_bytes()).await {
                    std::io::stderr()
                        .write_fmt(format_args!("{:?}\n", err))
                        .unwrap_or_default();
                }
            }
            Err(ReadlineError::Interrupted) => {
                std::io::stderr().write_all(b"CTRL-C\n").unwrap_or_default();
                break;
            }
            Err(ReadlineError::Eof) => {
                std::io::stderr().write_all(b"CTRL-D\n").unwrap_or_default();
                break;
            }
            Err(err) => {
                std::io::stderr()
                    .write_fmt(format_args!("{:?}\n", err))
                    .unwrap_or_default();
                break;
            }
        }
    }
}

fn o(msg: &str) -> Result<()> {
    std::io::stderr().write_all(msg.as_bytes())?;
    Ok(())
}

fn usage() -> Result<()> {
    o("usage: cmd [args]\n\n")?;
    o("commands: \n")?;
    o("\t help             \t show usage\n")?;
    o("\t config key value \t global parameters, supported [database, collection, create-if-missing]\n")?;
    o("\t get key [db <db-name>] [coll <co-name>]\n")?;
    o("\t put key value [db <db-name>] [coll <co-name>]\n")?;
    o("\t delete key [db <db-name>] [coll <co-name>]\n")?;
    Ok(())
}

async fn new_session(addrs: Vec<String>) -> Result<Session> {
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(200)),
        timeout: Some(Duration::from_millis(500)),
    };
    let client = EngulaClient::new(opts, addrs).await?;
    Ok(Session {
        client,
        config: HashMap::default(),
        databases: HashMap::default(),
        collections: HashMap::default(),
    })
}

fn skip_space(input: &[u8]) -> &[u8] {
    for i in 0..input.len() {
        if input[i] != b' ' && input[i] != b'\t' && input[i] != 10 {
            return &input[i..];
        }
    }
    &input[input.len()..]
}

fn is_eof(input: &[u8]) -> bool {
    skip_space(input).is_empty()
}

fn next_token(input: &[u8]) -> Option<(&[u8], Token)> {
    read_entry(input)
        .and_then(|(input, value)| global_token_map().get(&value).map(|tok| (input, *tok)))
}

fn expect_token(input: &[u8], tok: Token) -> Option<&[u8]> {
    if let Some((input, value)) = read_entry(input) {
        match global_token_map().get(&value) {
            Some(got_tok) if *got_tok == tok => {
                return Some(input);
            }
            _ => {}
        }
    }
    None
}

fn must_eof(input: &[u8]) -> ParseResult<()> {
    let input = skip_space(input);
    if !is_eof(input) {
        Err(format!(
            "unexpected sequence: {}",
            std::str::from_utf8(input).expect("invalid UTF-8 sequences")
        ))
    } else {
        Ok(())
    }
}

fn read_entry(input: &[u8]) -> Option<(&[u8], Vec<u8>)> {
    let mut entry = vec![];
    let len = input.len();
    let mut i = 0;
    while i < len {
        if input[i] == b'\\' {
            if i + 1 == len {
                println!("unexpected escape, found EOF");
                return None;
            }
            match input[i + 1] {
                b'n' => entry.push(b'\n'),
                b't' => entry.push(b'\t'),
                b'r' => entry.push(b'\r'),
                b' ' => entry.push(b' '),
                b'\\' => entry.push(b'\\'),
                _ => {
                    println!("unknown escape: {:?}", input[i + 1]);
                    return None;
                }
            }
            i += 2;
            continue;
        } else if 33 <= input[i] && input[i] < 127 {
            // from ! to `
            entry.push(input[i]);
            i += 1;
            continue;
        } else {
            return Some((&input[i..], entry));
        }
    }
    if entry.is_empty() {
        None
    } else {
        Some((&input[input.len()..], entry))
    }
}

lazy_static! {
    static ref INSTANCE: HashMap<Vec<u8>, Token> = {
        let mut m = HashMap::new();
        m.insert(Vec::from(&b"get"[..]), Token::Get);
        m.insert(Vec::from(&b"put"[..]), Token::Put);
        m.insert(Vec::from(&b"delete"[..]), Token::Delete);
        m.insert(Vec::from(&b"help"[..]), Token::Help);
        m.insert(Vec::from(&b"config"[..]), Token::Config);
        m.insert(Vec::from(&b"db"[..]), Token::Db);
        m.insert(Vec::from(&b"coll"[..]), Token::Coll);
        m
    };
}

fn global_token_map() -> &'static HashMap<Vec<u8>, Token> {
    &INSTANCE
}
