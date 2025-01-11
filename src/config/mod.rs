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
    
    pub fn has_rdb(&self) -> bool {
        // println!("dir: {:?}, dbfilename: {:?}", self.dir, self.dbfilename);
        self.dir.is_some() && self.dbfilename.is_some()
    }

    pub fn get_rdb_path(&self) -> Option<String> {
        if self.has_rdb() {
            return Some(format!(
                "{}/{}",
                self.dir.clone().unwrap(),
                self.dbfilename.clone().unwrap()
            ));
        }
        None
    }
}
