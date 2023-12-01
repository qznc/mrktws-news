use metaculustetra::Metaculus;

enum Market {
    Polymarket,
    Metaculus,
    Manifold,
    GJOpen,
    Kalshi,
    Augur,
    Infer,
    Range,
    Insight,
    PredictIt,
    IEM,
    HSX,
    Foresight,
    Hypermind,
}

fn main() {
    println!("Hello, world!");

    let m = Metaculus::standard();
    let q = m.get_question("3682").expect("got question");
    let p = q.get_best_prediction().expect("got prediction");
    println!("M {}: {:?}", q.title_short, p)
}
