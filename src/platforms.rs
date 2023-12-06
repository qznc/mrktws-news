use chrono::prelude::*;
use log::*;
use std::fmt;

#[derive(Debug)]
pub enum Platform {
    Polymarket,
    Metaculus,
    Manifold,
    _GJOpen,
    _Kalshi,
    _Augur,
    _Infer,
    _Range,
    _Insight,
    _PredictIt,
    _IEM,
    _HSX,
    _Foresight,
    _Hypermind,
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Platform::Polymarket => write!(f, "Polymarket"),
            Platform::Metaculus => write!(f, "Metaculus"),
            Platform::Manifold => write!(f, "Manifold"),
            _ => write!(f, "???"),
        }
    }
}

pub fn _get_platform(p: &String) -> Option<Box<dyn PlatformAPI>> {
    match p.as_str() {
        "Polymarket" => Option::Some(Box::new(Polymarket {})),
        "Metaculus" => Option::Some(Box::new(Metaculus {})),
        "Manifold" => Option::Some(Box::new(Manifold {})),
        _ => Option::None,
    }
}

pub trait PlatformAPI {
    fn id(&self) -> Platform;
    fn some_markets(&self) -> Vec<MarketStatus>;
}

#[derive(Debug)]
pub struct MarketStatus {
    pub platform: Platform,
    pub id: String,
    pub prob: f32,
    pub time: DateTime<Utc>,
    pub url: String,
    pub title: String,
}

pub struct Manifold {}

impl PlatformAPI for Manifold {
    fn id(self: &Self) -> Platform {
        Platform::Manifold
    }
    fn some_markets(&self) -> Vec<MarketStatus> {
        let url = "https://manifold.markets/api/v0/search-markets?limit=50&sort=last-updated&term=";
        let response = reqwest::blocking::get(url).unwrap().text().expect("body");
        let mut ret = vec![];
        if let Ok(j) = json::parse(response.as_str()) {
            for o in j.members() {
                if 20 > o["uniqueBettorCount"].as_i32().expect("bettor count") {
                    continue; // not enough bettors
                }
                if 200.0 > o["volume"].as_f32().expect("volume") {
                    continue; // not enough volume
                }
                let id = o["id"].to_string();
                let url = o["url"].to_string();
                let title = o["question"].to_string();
                let t = o["lastBetTime"].as_f64().expect("timestamp") as i64;
                let time = DateTime::from_timestamp(t / 1000, 0).expect("timestamp");
                let outcome_type = o["outcomeType"].as_str().expect("outcome type");
                match outcome_type {
                    "BINARY" => {
                        let prob = o["probability"].as_f32().unwrap_or(-1.0);
                        let status = MarketStatus {
                            platform: Platform::Manifold,
                            id,
                            prob,
                            time,
                            url,
                            title,
                        };
                        ret.push(status);
                    }
                    "FREE_RESPONSE" | "MULTIPLE_CHOICE" => {
                        let url = format!("https://manifold.markets/api/v0/market/{}", id);
                        let response = reqwest::blocking::get(&url).unwrap().text().expect("body");
                        if let Ok(d) = json::parse(response.as_str()) {
                            for a in d["answers"].members() {
                                let a_title = a["text"].to_string();
                                let prob = a["probability"].as_f32().unwrap_or(-1.0);
                                let status = MarketStatus {
                                    platform: Platform::Manifold,
                                    id: id.clone(),
                                    prob,
                                    time,
                                    url: url.clone(),
                                    title: format!("{} {}", title, a_title),
                                };
                                ret.push(status);
                            }
                        }
                    }
                    _ => {
                        debug!("Unhandle outcome type {}", outcome_type);
                        continue;
                    }
                }
            }
        } else {
            dbg!(response);
        };
        ret
    }
}

pub struct Metaculus {}

impl PlatformAPI for Metaculus {
    fn id(self: &Self) -> Platform {
        Platform::Metaculus
    }
    fn some_markets(&self) -> Vec<MarketStatus> {
        let url = "https://www.metaculus.com/api2/questions/?forecast_type=binary&type=forecast&limit=10&order_by=-activity&status=open";
        let response = reqwest::blocking::get(url).unwrap().text().expect("body");
        let mut ret = vec![];
        if let Ok(j) = json::parse(response.as_str()) {
            for o in j["results"].members() {
                let _question = o["title"].clone();
                let _traders = o["number_of_forecasters"].clone();
                let prob = o["community_prediction"]["full"]["q2"]
                    .as_f32()
                    .unwrap_or(-1.0);
                let id = o["id"].to_string();
                let t = o["last_activity_time"]
                    .as_str()
                    .expect("last_activity_time");
                let time: DateTime<Utc> = DateTime::parse_from_rfc3339(t)
                    .expect("iso8601")
                    .with_timezone(&Utc);
                let url = o["url"].to_string();
                let title = o["title"].to_string();
                let status = MarketStatus {
                    platform: self.id(),
                    id,
                    prob,
                    time,
                    url,
                    title,
                };
                ret.push(status);
            }
        } else {
            dbg!(response);
        };
        ret
    }
}

pub struct Polymarket {}

impl PlatformAPI for Polymarket {
    fn id(self: &Self) -> Platform {
        Platform::Polymarket
    }
    fn some_markets(&self) -> Vec<MarketStatus> {
        let mut ret = vec![];
        let query = r#"{ markets(limit: 10, order: "liquidity DESC")
                       { question, outcomePrices, slug, volume24hr, liquidity, updatedAt} }"#;
        let json_query = format!(
            r#"{{"query": "{}"}}"#,
            query.replace(r#"""#, r#"\""#).replace("\n", "")
        );
        let graphql_endpoint = "https://gamma-api.polymarket.com/query";
        let client = reqwest::blocking::Client::new();
        let response = client
            .post(graphql_endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(json_query)
            .send()
            .expect("response")
            .text()
            .expect("text body");
        if let Ok(j) = json::parse(response.as_str()) {
            for o in j["data"]["markets"].members() {
                let _traders = o["liquidity"].clone();
                let prices = json::parse(o["outcomePrices"].as_str().unwrap()).expect("valid json");
                let prob = prices[0].to_string().parse::<f32>().expect("parsed float");
                let id = o["slug"].to_string();
                let t = o["updatedAt"].as_str().expect("updatedAt");
                let time: DateTime<Utc> = DateTime::parse_from_rfc3339(t)
                    .expect("iso8601")
                    .with_timezone(&Utc);
                let url = "https://polymarket.com/event/https://polymarket.com/event/".to_string()
                    + id.as_str();
                if o["question"].is_null() {
                    debug!("Polymarket 'null question' drop: {:?}", j);
                    continue;
                }
                let title = o["question"].to_string();
                let status = MarketStatus {
                    platform: Platform::Polymarket,
                    id,
                    prob,
                    time,
                    url,
                    title,
                };
                ret.push(status);
            }
        } else {
            dbg!(response);
        };
        ret
    }
}
