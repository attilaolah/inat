use std::error::Error;

use clap::Parser;
use reqwest::{Client, Url};

/// CLI iNaturalist sync utility.
/// Stores a copy one's personal inaturalist data.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// iNat username.
    #[arg(short, long, env)]
    user: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let cli = Client::new();
    let url = format!("https://api.inaturalist.org/v1/users/{}", args.user);
    let res = cli.get(url.parse::<Url>()?).send().await?;

    if res.status().is_success() {
        let text = res.text().await?;
        println!("{}", text);
    } else {
        println!("ERR: {}", res.status());
    }

    Ok(())
}
