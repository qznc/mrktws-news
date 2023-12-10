mod mastodon;
mod model;
mod platforms;
use crate::mastodon::Mastodon;
use crate::model::*;
use crate::platforms::*;
use clap::{Arg, ArgAction, ArgMatches, Command};
use ini::Ini;
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
        .arg(
            Arg::new("publish")
                .long("no-publish")
                .action(ArgAction::SetFalse)
                .help("do not publish noteworthy change"),
        )
        .arg(
            Arg::new("ini")
                .long("config-file")
                .value_name("I")
                .default_value("mrktws.ini")
                .help("ini config file"),
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

    let tooter = get_tooter(&args);
    if args.get_flag("publish") {
        if let Some(q) = db.most_noteworthy_change() {
            let msg = as_change_str(&q);
            println!("Most noteworth change: {}", msg);
            let since = db.duration_since_last_publication();
            if since.num_hours() > 6 {
                tooter.expect("tooter").toot(msg);
                db.log_publication(q);
            } else {
                info!(
                    "Skip publication cause last one was only {} minutes ago.",
                    since.num_minutes()
                )
            }
        } else {
            info!("no noteworthy change");
        }
    } else {
        info!("skip publication");
    }
}

fn get_tooter(args: &ArgMatches) -> Option<Mastodon> {
    let ini_path = args.get_one::<String>("ini")?;
    let config = Ini::load_from_file(ini_path.as_str()).ok()?;
    let m_section = config.section(Some("mastodon"))?;
    let endpoint = m_section.get("api-endpoint")?;
    let access_token = m_section.get("access-token")?;
    let m_client = Mastodon::new(endpoint.to_string(), access_token.to_string());
    //m_client.toot("testing my bot implementation".to_string());
    Some(m_client)
}
