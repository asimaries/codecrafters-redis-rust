use clap::{command, Arg};

pub struct Config {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
}

impl Config {
    pub fn new() -> Self {
        let args = command!()
            .arg(Arg::new("dir").short('d').long("dir"))
            .arg(Arg::new("dbfilename").short('f').long("dbfilename"))
            .get_matches();

        Self {
            dir: args.get_one::<String>("dir").map(|d| d.to_owned()),
            dbfilename: args.get_one::<String>("dbfilename").map(|d| d.to_owned()),
        }
    }
}
