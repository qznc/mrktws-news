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
    pub fn update_prob(&self, market: &str, id: String, prob: f32) -> Result<f64, &str> {
        // first retrieve previous probability
        let check =
            "SELECT prob FROM probabilities WHERE market = ? AND id = ? ORDER BY time DESC LIMIT 1;";
        let mut s = self.c.prepare(check).expect("check first");
        s.bind((1, market)).expect("bind 1");
        s.bind((2, id.as_str())).expect("bind 2");
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
        stmt.bind((3, id.as_str())).expect("bind 3");
        stmt.next().expect("bind");
        prev_prob
    }

    pub fn outdated_questions(&self) -> Vec<(String, String)> {
        let mut ret: Vec<(String, String)> = vec![];
        let q = "SELECT market, id, MAX(time) AS last_update_time
FROM probabilities
GROUP BY market, id
ORDER BY last_update_time ASC
LIMIT 2;";
        let mut s = self.c.prepare(q).expect("query bound");
        while let Ok(sqlite::State::Row) = s.next() {
            let market = s.read::<String, _>("market").expect("market field");
            let id = s.read::<String, _>("id").expect("id field");
            ret.push((market, id));
        }
        ret
    }

    pub fn biggest_changes_daily(&self) -> Vec<(String, String)> {
        let mut ret: Vec<(String, String)> = vec![];
        let q = "
SELECT t1.market, t1.id, t1.prob AS prob1, t2.prob AS prob2, t2.prob - t1.prob AS difference
FROM probabilities t1
JOIN probabilities t2 ON t1.market = t2.market AND t1.id = t2.id
WHERE t2.time > DATETIME('now', '-1 day')
AND t1.time = (SELECT MAX(time) FROM probabilities WHERE market = t1.market AND id = t1.id)
AND t1.time < t2.time
ORDER BY difference DESC
LIMIT 1;";

        let mut s = self.c.prepare(q).expect("query bound");
        while let Ok(sqlite::State::Row) = s.next() {
            let market = s.read::<String, _>("market").expect("market field");
            let id = s.read::<String, _>("id").expect("id field");
            ret.push((market, id));
        }
        ret
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
    CREATE TABLE probabilities(time DATETIME DEFAULT CURRENT_TIMESTAMP, market TEXT, id TEXT, prob REAL);
";
    c.execute(query).expect("sql init");
}
