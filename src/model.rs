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
    ) -> Result<f64, &str> {
        // first retrieve previous probability
        let check =
            "SELECT prob FROM probabilities WHERE platform = ? AND id = ? ORDER BY time DESC LIMIT 1;";
        let mut s = self.c.prepare(check).expect("check first");
        s.bind((1, platform)).expect("bind 1");
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
        let query = "INSERT INTO probabilities (prob,platform,id) VALUES (?,?,?);";
        let mut stmt = self.c.prepare(query).expect("prepare prob update");
        stmt.bind((1, prob as f64)).expect("bind 1");
        stmt.bind((2, platform)).expect("bind 2");
        stmt.bind((3, id.as_str())).expect("bind 3");
        stmt.next().expect("bind");
        // save details
        let query = "INSERT OR REPLACE INTO details (platform,id,title,url) VALUES(?,?,?,?);";
        let mut stmt = self.c.prepare(query).expect("prepare detail update");
        stmt.bind((1, platform)).expect("bind 1");
        stmt.bind((2, id.as_str())).expect("bind 2");
        stmt.bind((3, title.as_str())).expect("bind 3");
        stmt.bind((4, url.as_str())).expect("bind 4");
        stmt.next().expect("bind");
        prev_prob
    }

    pub fn most_noteworthy_change(&self) -> Option<Change> {
        let mut most_noteworthy = Change::new(Duration::Week, 0.5);
        let timestamps = query_timestamps(&self.c);
        info!("found {} candidates for news", timestamps.len());
        for ts in timestamps {
            let plat = &ts.platform;
            let p_now = get_prob_by_time(&self.c, plat, &ts.id, &ts.latest).expect("latest prob");
            let c_hour = timestamp_to_change(&self.c, &ts, p_now, ts.hour.clone(), Duration::Hour);
            if let Some(c) = c_hour {
                if c > most_noteworthy {
                    most_noteworthy = c;
                }
            }
            let c_day = timestamp_to_change(&self.c, &ts, p_now, ts.day.clone(), Duration::Day);
            if let Some(c) = c_day {
                if c > most_noteworthy {
                    most_noteworthy = c;
                }
            }
            let c_week = timestamp_to_change(&self.c, &ts, p_now, ts.week.clone(), Duration::Week);
            if let Some(c) = c_week {
                if c > most_noteworthy {
                    most_noteworthy = c;
                }
            }
        }
        if most_noteworthy.url == "url" {
            Option::None
        } else {
            Option::Some(most_noteworthy)
        }
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum Duration {
    Hour,
    Day,
    Week,
}

#[derive(PartialEq, Debug)]
pub struct Change {
    platform: String,
    id: String,
    duration: Duration,
    p_before: f32,
    p_after: f32,
    url: String,
    title: String,
}

impl PartialOrd for Change {
    fn partial_cmp(&self, other: &Change) -> Option<std::cmp::Ordering> {
        let diff_left = (self.p_after - self.p_before).abs() * diff_factor(self.duration);
        let diff_right = (other.p_after - other.p_before).abs() * diff_factor(other.duration);
        diff_left.partial_cmp(&diff_right)
    }
}

impl Change {
    fn new(duration: Duration, p_after: f32) -> Self {
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
}

fn diff_factor(d: Duration) -> f32 {
    match d {
        Duration::Hour => 10.0,
        Duration::Day => 5.0,
        Duration::Week => 2.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn change_comparison() {
        let a = Change::new(Duration::Day, 0.4);
        let b = Change::new(Duration::Day, 0.1);
        assert!(a < b);
        let b = Change::new(Duration::Day, 0.9);
        assert!(a < b);
        let b = Change::new(Duration::Hour, 0.45);
        assert!(a < b);
        let b = Change::new(Duration::Week, 0.1);
        assert!(a < b);
    }
}

pub fn as_change_str(c: &Change) -> String {
    format!(
        "{:.1}% in {}: {}\n{}",
        100.0 * (c.p_after - c.p_before),
        match c.duration {
            Duration::Hour => "an hour",
            Duration::Day => "a day",
            Duration::Week => "a week",
        },
        c.title,
        c.url,
    )
}

fn timestamp_to_change(
    c: &Connection,
    timestamp: &Timestamps,
    p_now: f32,
    t: Option<String>,
    duration: Duration,
) -> Option<Change> {
    let ts = t?;
    let platform = timestamp.platform.as_str();
    let id = timestamp.id.as_str();
    let p_before = get_prob_by_time(c, platform, id, &ts)?;
    let u_t = get_details(c, platform, id);
    Option::Some(Change {
        platform: timestamp.platform.clone(),
        id: timestamp.id.clone(),
        duration,
        p_before,
        p_after: p_now,
        url: u_t.0,
        title: u_t.1,
    })
}

struct Timestamps {
    platform: String,
    id: String,
    latest: String,
    hour: Option<String>,
    day: Option<String>,
    week: Option<String>,
}

fn get_prob_by_time(c: &Connection, platform: &str, id: &str, time: &str) -> Option<f32> {
    let query = "SELECT prob FROM probabilities WHERE platform=? AND id=? AND time=?;";
    let mut s = c.prepare(query).expect("prepare");
    s.bind((1, platform)).expect("bind 1");
    s.bind((2, id)).expect("bind 2");
    s.bind((3, time)).expect("bind 3");
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

fn query_timestamps(c: &Connection) -> Vec<Timestamps> {
    let mut ret = vec![];
    let query = format!(
        " SELECT platform, id,
MAX(time) AS latest_time,
MAX(CASE WHEN time >= DATETIME(CURRENT_TIMESTAMP, '-45 minutes') AND time <= DATETIME(CURRENT_TIMESTAMP, '-69 minutes')THEN time END) AS time_1_hour_ago,
MAX(CASE WHEN time >= DATETIME(CURRENT_TIMESTAMP, '-22 hours') AND time <= DATETIME(CURRENT_TIMESTAMP, '-28 hours') THEN time END) AS time_1_day_ago,
MAX(CASE WHEN time >= DATETIME(CURRENT_TIMESTAMP, '-6 day') AND time <= DATETIME(CURRENT_TIMESTAMP, '-8 days') THEN time END) AS time_1_week_ago
FROM probabilities
GROUP BY platform, id
HAVING latest_time <> time_1_hour_ago; "
    );
    let mut s = c.prepare(query).expect("query bound");
    while let Ok(sqlite::State::Row) = s.next() {
        let timestamps = Timestamps {
            platform: s.read::<String, _>("platform").expect("field"),
            id: s.read::<String, _>("id").expect("id field"),
            latest: s.read::<String, _>("latest_time").expect("field"),
            hour: s.read::<String, _>("time_1_hour_ago").ok(),
            day: s.read::<String, _>("time_1_day_ago").ok(),
            week: s.read::<String, _>("time_1_week_ago").ok(),
        };
        ret.push(timestamps);
    }
    ret
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
    CREATE TABLE details (platform TEXT, id TEXT, title TEXT, url TEXT);
";
    c.execute(query).expect("sql init");
}
