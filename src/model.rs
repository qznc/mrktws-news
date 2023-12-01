use metaculustetra::Metaculus;
use sqlite::Connection;

pub struct Model {
    c: Connection,
}

impl Model {
    pub fn new(path: &str) -> Self {
        let db = Model {
            c: sqlite::open(path).unwrap(),
        };
        init_tables(&db.c);
        db
    }

    /// Archive new probability info
    /// Returns previous probability
    pub fn update_prob(&self, m: Market, id: &str, prob: f32) -> Result<f64, &str> {
        let market = match m {
            Market::Polymarket => "Polymarket",
            Market::Metaculus => "Metaculus",
            Market::Manifold => "Manifold",
            _ => "market?",
        };
        // first retrieve previous probability
        let check =
            "SELECT prob FROM probabilities WHERE market = ? AND id = ? ORDER BY time DESC LIMIT 1;";
        let mut s = self.c.prepare(check).expect("check first");
        s.bind((1, market)).expect("bind 1");
        s.bind((2, id)).expect("bind 2");
        let prev_prob = if let Ok(sqlite::State::Row) = s.next() {
            if let Ok(prob) = s.read::<f64, _>("prob") {
                Ok(prob)
            } else {
                Err("failed to retrieve probability")
            }
        } else {
            Err("no previous probability")
        };
        // now insert new probability
        let query = "INSERT INTO probabilities (prob,market,id) VALUES (?,?,?);";
        let mut stmt = self.c.prepare(query).expect("prepare prob update");
        stmt.bind((1, prob as f64)).expect("bind 1");
        stmt.bind((2, market)).expect("bind 2");
        stmt.bind((3, id)).expect("bind 3");
        stmt.next().expect("bind");
        prev_prob
    }
}

fn init_tables(c: &Connection) {
    let check_first_q = "SELECT name FROM sqlite_master WHERE type='table' AND name='log';";
    let mut s = c.prepare(check_first_q).expect("prep check");
    if let Ok(sqlite::State::Row) = s.next() {
        return; // already initalized
    }
    let query = "
    CREATE TABLE log (time DATETIME DEFAULT CURRENT_TIMESTAMP, type TEXT, content TEXT);
    INSERT INTO log (type, content) VALUES (\"creation\", \"hello world\");
    CREATE TABLE probabilities(time DATETIME DEFAULT CURRENT_TIMESTAMP, market TEXT, id TEXT, prob TEXT );
";
    c.execute(query).expect("sql init");
}

pub enum Market {
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
