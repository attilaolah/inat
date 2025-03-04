use std::collections::HashMap;

use httpdate::fmt_http_date;
use itertools::Itertools;
use reqwest::header::{DATE, ETAG, IF_MODIFIED_SINCE};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

use crate::{
    api::{
        expect_results, extract_id, extract_ids, fetch, is_last_page, lookup_cache_ids,
        write_cache, Api, ID,
    },
    error::{internal, Error},
    normalise::Normaliser,
};

// NOTE: Sometimes incorrectly documented as 500.
const MAX_IDS_PER_PAGE: usize = 200;

// NOTE: This is an educated guess; documented as 200;
const MAX_ITEMS_PER_PAGE: usize = 20;

impl Api {
    pub(crate) async fn sync_user_observations(&self, user_id: u64) -> Result<(), Error> {
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
            ("per_page", &MAX_IDS_PER_PAGE.to_string()),
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

        for chunk in &ids.into_iter().chunks(MAX_ITEMS_PER_PAGE) {
            let ids: Vec<u64> = chunk.collect();
            self.sync_observations(&ids).await?;
        }

        Ok(())
    }

    async fn sync_observations(&self, ids: &[u64]) -> Result<(), Error> {
        let (mut header, res) = fetch(self.client.get(self.endpoint(&format!(
            "/observations/{}",
            ids.iter().map(|id| id.to_string()).join(",")
        ))))
        .await?
        .ok_or(internal(&format!(
            "observations ({}): no response",
            ids.len()
        )))?;

        // The header can be used for each individual item.
        // But the etag doesn't match single items, so remove it.
        header.remove(YamlValue::String(ETAG.to_string()));

        let observations = expect_results(res)?
            .into_iter()
            .map(|obs| extract_id(&obs).and_then(|id| Ok((id, obs))))
            .collect::<Result<HashMap<_, _>, _>>()?;

        Normaliser::new(header, observations, &self.data_dir).write()
    }
}
