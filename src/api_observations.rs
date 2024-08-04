use std::{path::Path, sync::Arc};

use httpdate::fmt_http_date;
use reqwest::{
    header::{DATE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH},
    Client, Url,
};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};
use tokio::{
    select,
    sync::{mpsc, Semaphore},
    task::{spawn, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::api::{
    extract_ids, extract_single_value, fetch, is_last_page, lookup_cache_id, lookup_cache_ids,
    write_cache, Api, ID, MAX_PER_PAGE, MAX_WORKERS,
};
use crate::error::Error;

impl Api {
    pub(crate) async fn sync_observation_ids(&self, user_id: u64) -> Result<Vec<u64>, Error> {
        let mut ids: Vec<u64> = vec![];
        let mut last_header = YamlMapping::new();
        let cache_path = self
            .path("users")
            .join(format!("{}.observations.yaml", user_id));

        let mut url = self.endpoint("/observations");
        for (key, val) in [
            // keep sorted
            ("only_id", "true"),
            ("order", "asc"),
            ("order_by", ID),
            ("per_page", MAX_PER_PAGE),
            ("user_id", &user_id.to_string()),
        ] {
            url.query_pairs_mut().append_pair(key, val);
        }

        let last_modified = lookup_cache_ids(&cache_path)?.map(|cached| {
            ids = cached.ids;
            last_header.insert(
                YamlValue::String(DATE.to_string()),
                YamlValue::String(cached.header.date.to_rfc3339()),
            );
            cached.header.date
        });

        loop {
            let mut url = url.clone();
            if let Some(id) = ids.last() {
                url.query_pairs_mut()
                    .append_pair("id_above", &id.to_string());
            }

            let mut req = self.client.get(url);
            if let Some(date) = last_modified {
                req = req.header(IF_MODIFIED_SINCE, fmt_http_date(date.into()));
            }

            let (mut header, res) = match fetch(req).await? {
                Some(val) => val,
                _ => break, // cache hit
            };

            let is_last = is_last_page(&res)?;
            ids.extend_from_slice(&extract_ids(res)?);

            if is_last {
                // No need to store the etag since it won't be used.
                header.remove(YamlValue::String(ETAG.to_string()));
                last_header = header;
                break;
            }
        }

        // TODO: handle deleted observations!
        // If we receive no updates, but we end up having more IDs than listed in the users object,
        // we need to re-fetch all IDs to make sure we get rid of the deleted ones.

        write_cache(&cache_path, &last_header, &ids)?;

        Ok(ids)
    }

    pub(crate) async fn sync_observations(&self, ids: &[u64]) -> Result<(), Error> {
        let sem = Arc::new(Semaphore::new(MAX_WORKERS));
        let (err_tx, mut err_rx) = mpsc::channel(MAX_WORKERS);
        let cancel = Arc::new(CancellationToken::new());

        let tasks: Vec<JoinHandle<()>> = ids
            .to_vec()
            .into_iter()
            .map(|id| {
                let sem = sem.clone();
                let err_tx = err_tx.clone();
                let cancel = Arc::clone(&cancel);
                let client = self.client.clone();
                let cache_path = self.path("observations").join(format!("{}.yaml", id));
                let url = self.endpoint(&format!("/observations/{}", id));


                spawn(async move {
                    select! {
                        _ = cancel.cancelled() => {}
                        _ = async {
                            match sem.acquire().await {
                                Ok(permit) => {
                                    if let Err(err) = sync_observation(client, &cache_path, url, id).await {
                                        if let Err(err) = err_tx.send(err).await {
                                            error!("observations/{}: send error: {}", id, err);
                                        }
                                    }
                                    drop(permit); // todo: not needed here?
                                }
                                Err(err) => if let Err(err) = err_tx.send(Error::AcquireError(err)).await {
                                    error!("observations/{}: send error: {}", id, err);
                                }
                            }
                        } => {}
                    }
                })
            })
            .collect();

        select! {
            _ = async {
                for task in tasks {
                    if let Err(err) = task.await {
                        if let Err(err) = err_tx.send(Error::JoinError(err)).await {
                            error!("observations: send error: {}", err);
                        }
                    }
                }

                // Drop the tx channel.
                // This makes sure the receiver end can terminate.
                drop(err_tx);
            } => {
                info!("observations synced");
                Ok(())
            }
            Some(err) = err_rx.recv() => {
                error!("observations failed to sync: {}; cancelling tasks", err);
                cancel.cancel();
                Err(err)
            }
        }
    }
}

async fn sync_observation(
    client: Client,
    cache_path: &Path,
    url: Url,
    id: u64,
) -> Result<(), Error> {
    info!("observations/{}: syncing", id);

    let mut req = client.get(url);
    if let Some(cache) = lookup_cache_id(&cache_path)? {
        req = req.header(IF_MODIFIED_SINCE, fmt_http_date(cache.header.date.into()));
        if let Some(etag) = cache.header.etag {
            req = req.header(IF_NONE_MATCH, etag);
        }
    }

    let (header, res) = match fetch(req).await? {
        Some(val) => val,
        _ => {
            info!("observations/{}: cache hit", id);
            return Ok(()); // cache hit
        }
    };

    write_cache(&cache_path, &header, &extract_single_value(res)?)?;
    info!("observations/{}: updated", id);

    Ok(())
}
