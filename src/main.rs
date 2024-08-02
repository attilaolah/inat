use clap::Parser;
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use thiserror::Error;

/// CLI iNaturalist sync utility.
/// Stores a copy one's personal inaturalist data.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// iNat username.
    #[arg(short, long, env)]
    user: String,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    error: Option<String>,
}

#[derive(Error, Debug)]
enum ResponseError {
    #[error("status {0}: {1}")]
    BadStatus(StatusCode, String),

    #[error(transparent)]
    BadUrl(#[from] url::ParseError),

    #[error(transparent)]
    Failed(#[from] reqwest::Error),
}

#[tokio::main]
async fn main() -> Result<(), ResponseError> {
    let args = Args::parse();

    let cli = Client::new();
    let url = format!("https://api.inaturalist.org/v1/users/{}", args.user);
    let res = cli.get(url.parse::<Url>()?).send().await?;

    if res.status().is_success() {
        let text = res.text().await?;
        println!("{}", text);
        Ok(())
    } else {
        Err(ResponseError::BadStatus(
            res.status(),
            extract_error(&res.text().await.unwrap_or("".to_string())),
        ))
    }
}

fn extract_error(data: &str) -> String {
    let res: ApiError = serde_json::from_str(data).unwrap_or(ApiError { error: None });
    res.error.unwrap_or_else(|| data.to_string())
}
