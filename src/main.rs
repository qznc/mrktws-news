mod model;
mod platforms;
use crate::model::*;
use crate::platforms::*;
use clap::{Arg, ArgAction, Command};
use log::*;

fn arguments() -> Command {
    Command::new("marketwise-news")
        .version("0.1")
        .arg(
            Arg::new("database")
                .long("database")
                .default_value(":memory:")
                .help("SQLite database to use"),
        )
        .arg(
            Arg::new("get_some")
                .long("get-some")
                .action(ArgAction::SetTrue)
                .help("fetch random market info"),
        )
}

fn main() {
    env_logger::init();
    let args = arguments().get_matches();
    let db = Model::new(args.get_one::<String>("database").unwrap());
    let platforms: Vec<Box<dyn PlatformAPI>> = match args.get_flag("get_some") {
        true => {
            vec![
                Box::new(Manifold {}),
                Box::new(Metaculus {}),
                Box::new(Polymarket {}),
            ]
        }
        false => {
            vec![]
        }
    };

    for p in platforms {
        for s in p.some_markets() {
            let p = s.platform.to_string();
            if s.prob >= 0.0 && s.prob <= 1.0 {
                //info!("update {} '{}' {:.1}%", p, s.title, s.prob * 100.0);
                if let Ok(_f64) = db.update_prob(s.time, p.as_str(), s.id, s.prob, s.url, s.title) {
                }
            } else {
                debug!("ignore {} '{}' {}", p, s.title, s.prob);
            }
        }
    }

    if let Some(q) = db.most_noteworthy_change() {
        info!("change! {:?}", q);
        println!("Most noteworth change: {}", as_change_str(&q));
        // TODO report news
    }
}
