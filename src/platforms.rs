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

pub fn get_platform(p: &String) -> Option<Box<dyn PlatformAPI>> {
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
    fn update_market(&self, id: &String) -> Option<MarketStatus>;
}

#[derive(Debug)]
pub struct MarketStatus {
    pub platform: Platform,
    pub id: String,
    pub prob: f32,
}

pub struct Manifold {}

impl PlatformAPI for Manifold {
    fn id(self: &Self) -> Platform {
        Platform::Manifold
    }
    fn some_markets(&self) -> Vec<MarketStatus> {
        let url = "https://manifold.markets/api/v0/search-markets?limit=10&sort=24-hour-vol&term=";
        let response = reqwest::blocking::get(url).unwrap().text().expect("body");
        let mut ret = vec![];
        if let Ok(j) = json::parse(response.as_str()) {
            for o in j.members() {
                let _question = o["question"].clone();
                let _traders = o["uniqueBettorCount"].clone();
                let prob = o["probability"].as_f32().unwrap_or(-1.0);
                let id = o["id"].to_string();
                let status = MarketStatus {
                    platform: Platform::Manifold,
                    id,
                    prob,
                };
                ret.push(status);
            }
        } else {
            dbg!(response);
        };
        ret
    }
    fn update_market(&self, id: &String) -> Option<MarketStatus> {
        let url = format!("https://manifold.markets/api/v0/market/{}", id);
        debug!("fetch {}", url);
        let response = reqwest::blocking::get(url).unwrap().text().expect("body");
        match json::parse(response.as_str()) {
            Ok(j) => {
                let prob = j["probability"].as_f32().unwrap();
                Option::Some(MarketStatus {
                    platform: self.id(),
                    id: id.clone(),
                    prob,
                })
            }
            Err(e) => {
                warn!("json parse failed: {}", e);
                Option::None
            }
        }
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
                let status = MarketStatus {
                    platform: self.id(),
                    id,
                    prob,
                };
                ret.push(status);
            }
        } else {
            dbg!(response);
        };
        ret
    }
    fn update_market(&self, id: &String) -> Option<MarketStatus> {
        let url = format!("https://www.metaculus.com/api2/questions/{}/", id);
        debug!("fetch {}", url);
        let response = reqwest::blocking::get(url).unwrap().text().expect("body");
        match json::parse(response.as_str()) {
            Ok(j) => {
                let prob = j["community_prediction"]["full"]["q2"]
                    .as_f32()
                    .unwrap_or(-1.0);
                Option::Some(MarketStatus {
                    platform: self.id(),
                    id: id.clone(),
                    prob,
                })
            }
            Err(e) => {
                warn!("json parse failed: {}", e);
                Option::None
            }
        }
    }
}

pub struct Polymarket {}

impl PlatformAPI for Polymarket {
    fn id(self: &Self) -> Platform {
        Platform::Polymarket
    }
    fn some_markets(&self) -> Vec<MarketStatus> {
        let mut ret = vec![];
        let query = r#"{ markets(limit: 10, order: "liquidity DESC") { question, outcomePrices, slug, volume24hr, liquidity } }"#;
        let json_query = format!(r#"{{"query": "{}"}}"#, query.replace(r#"""#, r#"\""#));
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
                let _question = o["question"].clone();
                let _traders = o["liquidity"].clone();
                let prices = json::parse(o["outcomePrices"].as_str().unwrap()).expect("valid json");
                let prob = prices[0].to_string().parse::<f32>().expect("parsed float");
                let id = o["slug"].to_string();
                let status = MarketStatus {
                    platform: Platform::Polymarket,
                    id,
                    prob,
                };
                ret.push(status);
            }
        } else {
            dbg!(response);
        };
        ret
    }
    fn update_market(&self, id: &String) -> Option<MarketStatus> {
        let query = r#"{ markets(limit: 1, where: "slug = 'XXX'") { question, outcomePrices, slug, volume24hr, liquidity } }"#;
        let json_query = format!(
            r#"{{"query": "{}"}}"#,
            query.replace(r#"""#, r#"\""#).replace("XXX", id)
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
        match json::parse(response.as_str()) {
            Ok(j) => {
                let prices =
                    json::parse(j["data"]["markets"][0]["outcomePrices"].as_str().unwrap())
                        .expect("valid json");
                let prob = prices[0].to_string().parse::<f32>().expect("parsed float");
                Option::Some(MarketStatus {
                    platform: self.id(),
                    id: id.clone(),
                    prob,
                })
            }
            Err(e) => {
                warn!("json parse failed: {}", e);
                Option::None
            }
        }
    }
}
