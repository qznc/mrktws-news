use chrono::prelude::*;
use log::*;
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

    pub fn transact(&self, f: &dyn Fn()) {
        debug!("transation begin");
        self.c.execute("BEGIN TRANSACTION;").expect("begin");
        f();
        self.c.execute("COMMIT;").expect("commit");
        debug!("transaction commit");
    }

    /// Archive new probability info
    /// Returns previous probability
    pub fn update_prob(
        &self,
        time: DateTime<Utc>,
        platform: &str,
        id: String,
        prob: f32,
        url: String,
        title: String,
    ) -> Option<f32> {
        let prev_prob = previous_probability(&self.c, &platform, &id);
        match insert_probability(&self.c, prob, platform, &id, &title, &url, &time) {
            Ok(_) => {}
            Err(e) => {
                warn!("failed to insert prob: {}", e)
                // we still continue...
            }
        };
        prev_prob
    }

    pub fn most_noteworthy_change(&self) -> Option<Change> {
        let mut most_noteworthy = Change::new_from05(DiffDuration::Week, 0.5);
        let previous = last_publications(&self.c);
        let ago = duration_since_last_update(&self.c).unwrap_or(chrono::Duration::minutes(1));
        info!("looking {} minutes ago", ago.num_minutes());
        let timestamps = query_timestamps(&self.c, ago);
        info!("found {} candidates for news", timestamps.len());
        for ts in timestamps {
            let plat = &ts.platform;
            let p_now = get_prob_by_time(&self.c, plat, &ts.id, &ts.latest).expect("latest prob");
            let c_hour = ts.as_change(&self.c, p_now, ts.hour.clone(), DiffDuration::Hour);
            set_if_not_published(&mut most_noteworthy, c_hour, &previous);
            let c_day = ts.as_change(&self.c, p_now, ts.day.clone(), DiffDuration::Day);
            set_if_not_published(&mut most_noteworthy, c_day, &previous);
            let c_week = ts.as_change(&self.c, p_now, ts.week.clone(), DiffDuration::Week);
            set_if_not_published(&mut most_noteworthy, c_week, &previous);
        }
        debug!(
            "note before {} and after {}",
            most_noteworthy.p_before, most_noteworthy.p_after
        );
        if most_noteworthy.url == "url" {
            info!("found nothing to even consider noteworthyness");
            Option::None
        } else if (most_noteworthy.p_after - most_noteworthy.p_before).abs() < 0.2 {
            info!("not even one 20% move");
            Option::None
        } else {
            Option::Some(most_noteworthy)
        }
    }
    pub fn log_publication(&self, c: Change) {
        let q = "INSERT INTO log (type, content) VALUES ('pub', ?);";
        // multiple-choice markets get a postfix for each answer
        // ignore the postfix for logging
        let id = c.id.split_ascii_whitespace().next().expect("some id");
        let mut s = self.c.prepare(q).expect("prep check");
        s.bind((1, format!("{} {}", c.platform, id).as_str()))
            .expect("bind");
        s.next().expect("execute");
        info!("log pub {} {}", c.platform, c.id);
    }
    pub fn duration_since_last_publication(&self) -> chrono::Duration {
        let query = "SELECT time FROM log ORDER BY time DESC LIMIT 1;";
        let mut s = self.c.prepare(query).expect("prepare");
        if let Ok(sqlite::State::Row) = s.next() {
            let t = s.read::<String, _>("time").expect("time");
            let naive = NaiveDateTime::parse_from_str(t.as_str(), "%Y-%m-%d %H:%M:%S");
            Utc::now() - naive.expect("parsed").and_utc()
        } else {
            chrono::Duration::zero()
        }
    }
}

impl Drop for Model {
    fn drop(&mut self) {
        // we only care about probablities from a week ago
        let query = "DELETE FROM probabilities WHERE time < datetime('now', '-10 days');";
        self.c.execute(query).expect("Delete old probabilities");

        // sqlite suggests to run this "once, just prior to closing each database connection"
        // https://www.sqlite.org/lang_analyze.html
        self.c.execute("PRAGMA optimize;").ok();
    }
}

fn duration_since_last_update(c: &Connection) -> Option<chrono::Duration> {
    let query = "SELECT time FROM probabilities ORDER BY time DESC LIMIT 1;";
    let mut s = c.prepare(query).ok()?;
    if let Ok(sqlite::State::Row) = s.next() {
        let t = s.read::<String, _>("time").ok()?;
        let naive = NaiveDateTime::parse_from_str(t.as_str(), "%Y-%m-%d %H:%M:%S");
        Some(Utc::now() - naive.expect("parsed").and_utc())
    } else {
        Option::None
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum DiffDuration {
    Hour,
    Day,
    Week,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Change {
    platform: String,
    id: String,
    duration: DiffDuration,
    p_before: f32,
    p_after: f32,
    url: String,
    title: String,
}

impl PartialOrd for Change {
    fn partial_cmp(&self, other: &Change) -> Option<std::cmp::Ordering> {
        let diff_left = (self.p_after - self.p_before).abs() * diff_factor(&self.duration);
        let diff_right = (other.p_after - other.p_before).abs() * diff_factor(&other.duration);
        diff_left.partial_cmp(&diff_right)
    }
}

impl Change {
    fn new_from05(duration: DiffDuration, p_after: f32) -> Self {
        Change {
            platform: "platform".to_string(),
            id: "id".to_string(),
            duration,
            p_before: 0.5,
            p_after,
            url: "url".to_string(),
            title: "title".to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        let diff = 100.0 * (self.p_after - self.p_before);
        let emoji = if diff >= 0.0 { "📈" } else { "📉" };
        format!(
            "{:+.0}% in {} {} {}\n{} #prediction #{}",
            diff,
            match self.duration {
                DiffDuration::Hour => "an hour",
                DiffDuration::Day => "a day",
                DiffDuration::Week => "a week",
            },
            emoji,
            self.title,
            self.url,
            self.platform,
        )
    }
}

fn diff_factor(d: &DiffDuration) -> f32 {
    match d {
        DiffDuration::Hour => 2.0 * 3.0,
        DiffDuration::Day => 2.0,
        DiffDuration::Week => 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn change_comparison() {
        let a = Change::new_from05(DiffDuration::Day, 0.45);
        let b = Change::new_from05(DiffDuration::Day, 0.44);
        assert!(a < b); // -5% < -6% change
        let c = Change::new_from05(DiffDuration::Day, 0.56);
        assert!(a < c); // -5% < +6% change
        let d = Change::new_from05(DiffDuration::Day, 0.46);
        assert!(a > d); // -5% < -4% change
        let e = Change::new_from05(DiffDuration::Hour, 0.47);
        assert!(a < e); // -5% day < -3% hour
        let f = Change::new_from05(DiffDuration::Week, 0.1);
        assert!(a < f); // -5% day < -40% week
    }
    #[test]
    fn change_comparison2() {
        let a = Change::new_from05(DiffDuration::Day, 0.7);
        let b = Change::new_from05(DiffDuration::Week, 0.89);
        assert!(a > b); // +20 day > +39% week
        let c = Change::new_from05(DiffDuration::Week, 0.91);
        assert!(a < c); // +20 day < +39% week
        let d = Change::new_from05(DiffDuration::Hour, 0.65);
        assert!(a < d); // +20% day < +15% hour
        let e = Change::new_from05(DiffDuration::Hour, 0.56);
        assert!(a > e); // +20% day > +6% hour
    }
}

fn insert_probability(
    c: &Connection,
    prob: f32,
    platform: &str,
    id: &str,
    title: &str,
    url: &str,
    time: &DateTime<Utc>,
) -> Result<String, sqlite::Error> {
    // now insert new probability
    let query = "INSERT INTO probabilities (prob,platform,id,time) VALUES (?,?,?,?);";
    let mut stmt = c.prepare(query)?;
    stmt.bind((1, prob as f64))?;
    stmt.bind((2, platform))?;
    stmt.bind((3, id))?;
    let t: String = time.format("%Y-%m-%d %H:%M:%S").to_string();
    stmt.bind((4, t.as_str()))?;
    stmt.next()?;
    // save details
    let query = "INSERT INTO details (platform,id,title,url) VALUES(?,?,?,?);";
    let mut stmt = c.prepare(query)?;
    stmt.bind((1, platform))?;
    stmt.bind((2, id))?;
    stmt.bind((3, title))?;
    stmt.bind((4, url))?;
    stmt.next()?;
    Result::Ok("good".to_string())
}

fn previous_probability(c: &Connection, platform: &str, id: &str) -> Option<f32> {
    let check =
        "SELECT prob FROM probabilities WHERE platform = ? AND id = ? ORDER BY time DESC LIMIT 1;";
    let mut s = c.prepare(check).ok()?;
    s.bind((1, platform)).ok()?;
    s.bind((2, id)).ok()?;
    if sqlite::State::Row == s.next().ok()? {
        Some(s.read::<f64, _>("prob").ok()? as f32)
    } else {
        Option::None
    }
}

#[derive(Debug)]
struct Timestamp {
    platform: String,
    id: String,
    latest: String,
    hour: Option<String>,
    day: Option<String>,
    week: Option<String>,
}

impl Timestamp {
    fn as_change(
        &self,
        c: &Connection,
        p_now: f32,
        t: Option<String>,
        duration: DiffDuration,
    ) -> Option<Change> {
        let ts = t?;
        let platform = self.platform.as_str();
        let id = self.id.as_str();
        let p_before = get_prob_by_time(c, platform, id, &ts)?;
        let u_t = get_details(c, platform, id);
        Option::Some(Change {
            platform: self.platform.clone(),
            id: self.id.clone(),
            duration,
            p_before,
            p_after: p_now,
            url: u_t.0,
            title: u_t.1,
        })
    }
}

fn get_prob_by_time(c: &Connection, platform: &str, id: &str, time: &str) -> Option<f32> {
    let query = "SELECT prob FROM probabilities WHERE platform=? AND id=? AND time=?;";
    let mut s = c.prepare(query).ok()?;
    s.bind((1, platform)).ok();
    s.bind((2, id)).ok();
    s.bind((3, time)).ok();
    if let Ok(sqlite::State::Row) = s.next() {
        if let Ok(prob) = s.read::<f64, _>("prob") {
            Option::Some(prob as f32)
        } else {
            Option::None
        }
    } else {
        Option::None
    }
}

fn get_details(c: &Connection, platform: &str, id: &str) -> (String, String) {
    let query = "SELECT url, title FROM details WHERE platform=? AND id=?;";
    let mut s = c.prepare(query).expect("prepare");
    s.bind((1, platform)).expect("bind 1");
    s.bind((2, id)).expect("bind 2");
    if let Ok(sqlite::State::Row) = s.next() {
        let url = s.read::<String, _>("url").expect("url");
        let title = s.read::<String, _>("title").expect("title");
        (url, title)
    } else {
        ("?".to_string(), "??".to_string())
    }
}

fn query_timestamps(c: &Connection, minutes_ago: chrono::Duration) -> Vec<Timestamp> {
    let mut ret = vec![];
    let min = minutes_ago.num_minutes();
    let query = format!(
        "SELECT platform, id,
MAX(CASE WHEN time >= DATETIME(CURRENT_TIMESTAMP, '-{} minutes') THEN time END) AS latest_time,
MAX(CASE WHEN time <= DATETIME(CURRENT_TIMESTAMP, '-{} minutes') AND time >= DATETIME(CURRENT_TIMESTAMP, '-{} minutes')THEN time END) AS time_1_hour_ago,
MAX(CASE WHEN time <= DATETIME(CURRENT_TIMESTAMP, '-22 hours') AND time >= DATETIME(CURRENT_TIMESTAMP, '-28 hours') THEN time END) AS time_1_day_ago,
MAX(CASE WHEN time <= DATETIME(CURRENT_TIMESTAMP, '-6 day') AND time >= DATETIME(CURRENT_TIMESTAMP, '-8 days') THEN time END) AS time_1_week_ago
FROM probabilities
GROUP BY platform, id;",
         min+10, min+55, min+99
    );
    let mut s = c.prepare(query).expect("query bound");
    while let Ok(sqlite::State::Row) = s.next() {
        let latest = match s.read::<String, _>("latest_time") {
            Result::Ok(x) => x,
            Result::Err(_) => {
                continue; // no latest value
            }
        };
        let hour = s.read::<String, _>("time_1_hour_ago").ok();
        let day = s.read::<String, _>("time_1_day_ago").ok();
        let week = s.read::<String, _>("time_1_week_ago").ok();
        if hour.is_none() && day.is_none() && week.is_none() {
            continue; // no previous data about this market
        }
        let timestamps = Timestamp {
            platform: s.read::<String, _>("platform").expect("field"),
            id: s.read::<String, _>("id").expect("id field"),
            latest,
            hour,
            day,
            week,
        };
        ret.push(timestamps);
    }
    ret
}

fn last_publications(c: &Connection) -> Vec<String> {
    let query = "SELECT content FROM log WHERE type = 'pub' ORDER BY time DESC LIMIT 30;";
    let mut s = c.prepare(query).expect("query bound");
    let mut ret: Vec<String> = vec![];
    while let Ok(sqlite::State::Row) = s.next() {
        ret.push(s.read::<String, _>("content").expect("content"));
    }
    ret
}

fn set_if_not_published(a: &mut Change, b: Option<Change>, previous: &Vec<String>) {
    if b.is_none() {
        return; // is none
    }
    let next = b.expect("is some");
    if a > &mut next.clone() {
        return; // a is better already
    }
    let next_id = format!("{} {}", next.platform, next.id);
    for p in previous {
        if next_id.starts_with(p) {
            return; // already published
        }
    }
    debug!("do set {}-{}", next.p_before, next.p_after);
    a.clone_from(&next);
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
    CREATE TABLE probabilities(time DATETIME DEFAULT CURRENT_TIMESTAMP, platform TEXT, id TEXT, prob REAL);
    CREATE INDEX idx_probabilities_platform_id_time ON probabilities(platform, id, time);
    CREATE TABLE details (platform TEXT, id TEXT, title TEXT, url TEXT);";
    c.execute(query).expect("sql init");
}
