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
            if s.prob > 0.0 && s.prob <= 1.0 {
                info!("update {} {} {}", p, s.id.clone(), s.prob);
                if let Ok(_f64) = db.update_prob(p.as_str(), s.id, s.prob) {}
            } else {
                debug!("ignore {} {} {}", p, s.id, s.prob);
            }
        }
    }

    for q in db.outdated_questions() {
        info!("outdated: {:?}", q);
        let p = get_platform(&q.0).expect("platform");
        let status = (*p).update_market(&q.1);
        match status {
            Some(s) => {
                let _ = db.update_prob(q.0.as_str(), s.id, s.prob);
            }
            None => {
                warn!("Updating failed: {:?}", q);
            }
        }
    }

    for q in db.biggest_changes_daily() {
        info!("change! {:?}", q);
        // TODO report news
    }
}
