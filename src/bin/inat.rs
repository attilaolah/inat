use clap::Parser;
use inat::{Api, ApiError};

/// CLI iNaturalist sync utility.
/// Stores a copy one's personal inaturalist data.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// iNat username.
    #[arg(short, long, env)]
    user: String,

    /// iNat API endpoint.
    #[arg(short, long, env, default_value = "https://api.inaturalist.org/v1")]
    endpoint: String,
}

#[tokio::main]
async fn main() -> Result<(), ApiError> {
    let args = Args::parse();
    let api = Api::new(&args.endpoint)?;

    println!("{}", api.user(&args.user).await?);

    Ok(())
}
