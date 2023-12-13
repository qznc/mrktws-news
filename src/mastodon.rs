use log::*;
use ureq::*;

pub struct Mastodon {
    endpoint: String,
    access_token: String,
}

impl Mastodon {
    pub fn new(endpoint: String, access_token: String) -> Self {
        Mastodon {
            endpoint,
            access_token,
        }
    }

    pub fn toot(&self, text: String) {
        let statuses = self.endpoint.clone() + "statuses/";
        let call = ureq::post(statuses.as_str())
            .set("Accept", "application/json")
            .set(
                "Authorization",
                format!("Bearer {}", self.access_token).as_str(),
            )
            .send_form(&[
                ("status", text.as_str()),
                ("visibility", "public"),
                ("language", "en"),
            ]);
        match call {
            Ok(_response) => {}
            Err(Error::Status(code, response)) => {
                debug!("error status {}: {:?}", code, response);
            }
            Err(_) => {
                error!("some kind of io/transport error");
            }
        };
    }
}
