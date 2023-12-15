use chrono::prelude::*;
use json::JsonValue;
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

pub struct Manifold {
    fetch_limit: i32,
}

impl Manifold {
    pub fn new_boxed(fetch_limit: i32) -> Box<dyn PlatformAPI> {
        Box::new(Manifold { fetch_limit })
    }
}

impl PlatformAPI for Manifold {
    fn id(self: &Self) -> Platform {
        Platform::Manifold
    }
    fn some_markets(&self) -> Vec<MarketStatus> {
        let url = format!(
            "https://api.manifold.markets/v0/search-markets?limit={}&sort=last-updated&term=",
            self.fetch_limit
        );
        let call = ureq::get(url.as_str()).call();
        let response = match call {
            Ok(c) => c.into_string().expect("body"),
            Err(e) => {
                warn!("{:?}", e);
                return vec![];
            }
        };
        let mut ret = vec![];
        if let Ok(j) = json::parse(response.as_str()) {
            for o in j.members() {
                if 25 > o["uniqueBettorCount"].as_i32().expect("bettor count") {
                    continue; // not enough bettors
                }
                if 400.0 > o["volume"].as_f32().expect("volume") {
                    continue; // not enough volume
                }
                let id = o["id"].to_string();
                let url = format!("{}?r=bWFya3R3c2U", o["url"]);
                let title = o["question"].to_string();
                let time = from_manifold_timestamp(o["lastBetTime"].as_f64());
                debug!("Manifold timestamp {:?}", time);
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
                    "MULTIPLE_CHOICE" | "FREE_RESPONSE" => {
                        let api_url = format!("https://api.manifold.markets/v0/market?id={}", id);
                        let call = ureq::get(api_url.as_str()).call();
                        let response = match call {
                            Ok(c) => c.into_string().expect("body"),
                            Err(e) => {
                                warn!("{:?}", e);
                                continue;
                            }
                        };
                        if let Ok(d) = json::parse(response.as_str()) {
                            for a in d["answers"].members() {
                                let a_title = a["text"].to_string();
                                let a_id = if a.has_key("index") {
                                    a["index"].as_f32().expect("index") as i32
                                } else if a.has_key("number") {
                                    a["number"].as_f32().expect("number") as i32
                                } else {
                                    error!("answer without index nor number: {:#?}", a);
                                    -1
                                };
                                let prob = a["probability"].as_f32().unwrap_or(-1.0);
                                let status = MarketStatus {
                                    platform: Platform::Manifold,
                                    id: format!("{} {}", id, a_id),
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
                        debug!("Unhandled outcome type {}", outcome_type);
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

fn from_manifold_timestamp(o: Option<f64>) -> DateTime<Utc> {
    let t = o.expect("timestamp");
    DateTime::from_timestamp(t as i64 / 1000, 0).expect("datetime")
}

pub struct Metaculus {
    fetch_limit: i32,
}

impl Metaculus {
    pub fn new_boxed(fetch_limit: i32) -> Box<dyn PlatformAPI> {
        Box::new(Metaculus { fetch_limit })
    }
}

impl PlatformAPI for Metaculus {
    fn id(self: &Self) -> Platform {
        Platform::Metaculus
    }
    fn some_markets(&self) -> Vec<MarketStatus> {
        let url = format!("https://www.metaculus.com/api2/questions/?forecast_type=binary&type=forecast&limit={}&order_by=-activity&status=open", self.fetch_limit);
        let call = ureq::get(url.as_str()).call();
        let response = match call {
            Ok(c) => c.into_string().expect("body"),
            Err(e) => {
                warn!("{:?}", e);
                return vec![];
            }
        };
        let mut ret = vec![];
        if let Ok(j) = json::parse(response.as_str()) {
            for o in j["results"].members() {
                let _question = o["title"].clone();
                if 30 > o["number_of_forecasters"].as_i32().expect("num casters") {
                    continue; // not enough forecasters
                };
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
                let url = o["url"].to_string().replace("api2/", "");
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

pub struct Polymarket {
    fetch_limit: i32,
}

impl Polymarket {
    pub fn new_boxed(fetch_limit: i32) -> Box<dyn PlatformAPI> {
        Box::new(Polymarket { fetch_limit })
    }
}

impl PlatformAPI for Polymarket {
    fn id(self: &Self) -> Platform {
        Platform::Polymarket
    }
    fn some_markets(&self) -> Vec<MarketStatus> {
        let mut ret = vec![];
        let query = format!(
            r#"{{ markets(limit: {}, order: "updated_at DESC")
                       {{ question, outcomePrices, slug, volume24hr, liquidity, updatedAt}} }}"#,
            self.fetch_limit
        );
        let json_query = format!(
            r#"{{"query": "{}"}}"#,
            query.replace(r#"""#, r#"\""#).replace("\n", "")
        );
        let graphql_endpoint = "https://gamma-api.polymarket.com/query";
        let call = ureq::post(graphql_endpoint)
            .set("Content-Type", "application/json")
            .send_string(json_query.as_str());
        let response = match call {
            Ok(c) => c.into_string().expect("body"),
            Err(e) => {
                warn!("{:?}", e);
                return vec![];
            }
        };
        if let Ok(j) = json::parse(response.as_str()) {
            for o in j["data"]["markets"].members() {
                if let Some(status) = parse_polymarket(o) {
                    ret.push(status);
                } else {
                    debug!("Polymarket drop: {:?}", o);
                }
            }
        } else {
            dbg!(response);
        };
        ret
    }
}

fn parse_polymarket(o: &JsonValue) -> Option<MarketStatus> {
    let volume24hr = o["volume24hr"].as_f32()?;
    let liquidity = o["liquidity"].to_string().parse::<f32>().ok()?;
    if liquidity < 500.0 || volume24hr < 10.0 {
        return None;
    };
    let prices = json::parse(o["outcomePrices"].as_str()?).ok()?;
    let prob = prices[0].to_string().parse::<f32>().ok()?;
    let id = o["slug"].to_string();
    let t = o["updatedAt"].as_str()?;
    let time: DateTime<Utc> = DateTime::parse_from_rfc3339(t).ok()?.with_timezone(&Utc);
    let url = "https://polymarket.com/event/".to_string() + id.as_str();
    if o["question"].is_null() {
        return Option::None;
    }
    let title = o["question"].to_string();
    let platform = Platform::Polymarket;
    Some(MarketStatus {
        platform,
        id,
        prob,
        time,
        url,
        title,
    })
}
