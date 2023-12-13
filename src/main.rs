mod mastodon;
mod model;
mod platforms;
use crate::mastodon::Mastodon;
use crate::model::*;
use crate::platforms::*;
use clap::{Arg, ArgAction, Command};
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
    info!("main start");

    let args = arguments().get_matches();
    let ini_path = args.get_one::<String>("ini").expect("ini");
    let config = Ini::load_from_file(ini_path.as_str()).ok();

    let db = Model::new(args.get_one::<String>("database").unwrap());
    let platforms: Vec<Box<dyn PlatformAPI>> = match args.get_flag("get_some") {
        true => {
            vec![
                Metaculus::new_boxed(get_fetch_limit(&config, "metaculus", 100)),
                Polymarket::new_boxed(get_fetch_limit(&config, "polymarket", 100)),
                Manifold::new_boxed(get_fetch_limit(&config, "manifold", 100)),
            ]
        }
        false => {
            vec![]
        }
    };
    for p in platforms {
        let ms = p.some_markets();
        info!("fetched {} markets from {}", ms.len(), p.id());
        for s in ms {
            let p = s.platform.to_string();
            if s.prob >= 0.0 && s.prob <= 1.0 {
                let t = s.title.clone();
                if let Some(_f64) = db.update_prob(s.time, p.as_str(), s.id, s.prob, s.url, s.title)
                {
                } else {
                    debug!("No prev prob {} '{}' {:.1}%", p, t, s.prob * 100.0);
                }
            } else {
                debug!("ignore {} '{}' {}", p, s.title, s.prob);
            }
        }
        info!("stored probabilities from {}", p.id());
    }
    info!("fetching part done");

    let tooter = get_tooter(config);
    if args.get_flag("publish") {
        if let Some(q) = db.most_noteworthy_change() {
            let msg = as_change_str(&q);
            println!("Most noteworthy change: {}", msg);
            let since = db.duration_since_last_publication();
            if since.num_hours() > 4 {
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

fn get_fetch_limit(config: &Option<Ini>, name: &str, default: i32) -> i32 {
    if config.is_none() {
        return default;
    }
    let c = config.as_ref().unwrap();
    let section = c.section(Some("fetch-limits"));
    if section.is_none() {
        return default;
    }
    let s = section.unwrap();
    let limit = s[name].parse::<i32>();
    limit.unwrap_or(default)
}

fn get_tooter(config: Option<Ini>) -> Option<Mastodon> {
    let c = config?;
    let m_section = c.section(Some("mastodon"))?;
    let endpoint = m_section.get("api-endpoint")?;
    let access_token = m_section.get("access-token")?;
    let m_client = Mastodon::new(endpoint.to_string(), access_token.to_string());
    //m_client.toot("testing my bot implementation".to_string());
    Some(m_client)
}
