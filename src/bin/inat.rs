use clap::Parser;
use inat::{Api, Error};
use tracing::{error, subscriber::set_global_default, Level};
use tracing_subscriber::FmtSubscriber;

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
async fn main() {
    set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .finish(),
    )
    .expect("failed to set global default subscriber");
    if let Err(err) = app().await {
        error!("{}", err);
    }
}

async fn app() -> Result<(), Error> {
    let args = Args::parse();
    let api = Api::new(&args.endpoint, &args.data)?;

    api.sync_all(&args.user).await?;

    Ok(())
}
