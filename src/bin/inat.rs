use clap::Parser;
use inat::{Api, Error};

/// CLI iNaturalist sync utility.
/// Stores a copy of one's personal inaturalist data.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// iNat username.
    #[arg(short, long, env)]
    user: String,

    /// iNat API endpoint.
    #[arg(short, long, env, default_value = "https://api.inaturalist.org/v1")]
    endpoint: String,

    /// Data directory for saving the results.
    #[arg(short, long, env, default_value = "data")]
    data: String,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let api = Api::new(&args.endpoint, &args.data)?;

    api.sync_user(&args.user).await?;

    Ok(())
}
