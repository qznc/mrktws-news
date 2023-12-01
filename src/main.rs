mod model;
use crate::model::*;

use metaculustetra::Metaculus;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let db = if args.len() == 2 {
        Model::new(args[1].as_str())
    } else {
        Model::new(":memory:")
    };
    //if let Ok(_f64) = db.update_prob(Market::Metaculus, "3682", 0.93) {}

    let m = Metaculus::standard();
    let q = m.get_question("3682").expect("got question");
    let p = q.get_best_prediction().expect("got prediction");
    println!("M {}: {:?}", q.title_short, p);

    let response = reqwest::blocking::get(
        "https://manifold.markets/api/v0/search-markets?limit=5&sort=24-hour-vol&term=",
    )
    .unwrap()
    .text()
    .expect("body");
    if let Ok(j) = json::parse(response.as_str()) {
        for o in j.members() {
            let question = o["question"].clone();
            let traders = o["uniqueBettorCount"].clone();
            let prob = o["probability"].as_f32().unwrap_or(-1.0);
            let id = o["slug"].as_str().unwrap_or("xxx");
            println!("{} {}", question, prob);
            if let Ok(_f64) = db.update_prob(Market::Manifold, id, prob) {}
        }
    } else {
        dbg!(response);
    };
}
