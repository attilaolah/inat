use std::collections::HashMap;

use httpdate::fmt_http_date;
use itertools::Itertools;
use reqwest::header::{DATE, ETAG, IF_MODIFIED_SINCE};
use serde_json::{Map as JsonMap, Value as JsonValue};
use serde_yaml::{Mapping as YamlMapping, Value as YamlValue};

use crate::api::{
    expect_results, extract_id, extract_ids, fetch, is_last_page, lookup_cache_ids, write_cache,
    Api, ID,
};
use crate::error::{internal, Error};

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

        let mut results = expect_results(res)?;

        // Normalise objects where applicable.
        self.extract_annotations(&mut results, &header)?;
        self.extract_application(&mut results, &header)?;
        self.extract_identifications(&mut results, &header)?;
        self.extract_observation_field_values(&mut results, &header)?;
        self.extract_observation_photos(&mut results, &header)?;
        self.extract_photos(&mut results, &header)?;
        self.extract_project_observations(&mut results, &header)?;
        self.extract_quality_metrics(&mut results, &header)?;
        self.extract_taxon(&mut results, &header)?;
        self.extract_user(&mut results, &header)?;
        self.extract_votes(&mut results, &header)?;

        for result in results {
            write_cache(
                &self
                    .path("observations")
                    .join(format!("{}.yaml", &extract_id(&result)?)),
                &header,
                &result,
            )?;
        }

        Ok(())
    }

    fn extract_application<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            if let Some((id, obj)) = extract_object(&mut result, "application")? {
                extracted.insert(id, obj);
            }
        }

        self.save_extracted(extracted, header, "applications")
    }

    fn extract_taxon<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for key in ["community_taxon", "taxon", "previous_observation_taxon"] {
                if let Some((id, mut obj)) = extract_object(&mut result, key)? {
                    for (id, obj) in extract_objects(&mut obj, "ancestors")? {
                        extracted.insert(id, obj);
                    }

                    extracted.insert(id, obj);
                }
            }
        }

        self.extract_photos(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "taxa")
    }

    fn extract_user<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for result in results {
            if let Some((id, obj)) = extract_object(result, "user")? {
                extracted.insert(id, obj);
            }
        }

        self.save_extracted(extracted, header, "users")
    }

    fn extract_annotations<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();

        for result in results {
            if let Some(annotations) = result.get_mut("annotations") {
                for annotation in annotations
                    .as_array_mut()
                    .ok_or(internal("annotations: not an array"))?
                    .iter_mut()
                    .map(|val| {
                        val.as_object_mut()
                            .ok_or(internal("annotations item: not an object"))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                {
                    for key in ["controlled_attribute", "controlled_value"] {
                        if let Some((id, mut obj)) = extract_object(annotation, key)? {
                            for (id, obj) in extract_objects(&mut obj, "values")? {
                                extracted.insert(id, obj);
                            }

                            extracted.insert(id, obj);
                        }
                    }

                    self.extract_user(vec![annotation], header)?;
                }
            }
        }

        self.extract_labels(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "controlled_terms")
    }

    fn extract_labels<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for (id, obj) in extract_objects(&mut result, "labels")? {
                extracted.insert(id, obj);
            }
        }

        self.save_extracted(extracted, header, "controlled_term_labels")
    }

    fn extract_identifications<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for key in ["identifications", "non_owner_ids"] {
                for (id, obj) in extract_objects(&mut result, key)? {
                    extracted.insert(id, obj);
                }
            }
        }

        self.extract_taxon(extracted.values_mut(), header)?;
        self.extract_user(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "identifications")
    }

    fn extract_photos<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for key in ["default_photo", "photo"] {
                if let Some((id, obj)) = extract_object(result, key)? {
                    extracted.insert(id, obj);
                }
            }

            for (id, obj) in extract_objects(&mut result, "photos")? {
                extracted.insert(id, obj);
            }
        }

        self.save_extracted(extracted, header, "photos")
    }

    fn extract_observation_photos<'a, T>(
        &self,
        results: T,
        header: &YamlMapping,
    ) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for (id, obj) in extract_objects(&mut result, "observation_photos")? {
                extracted.insert(id, obj);
            }
        }

        self.extract_photos(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "observation_photos")
    }

    fn extract_project_observations<'a, T>(
        &self,
        results: T,
        header: &YamlMapping,
    ) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for (id, obj) in extract_objects(&mut result, "project_observations")? {
                extracted.insert(id, obj);
            }
        }

        self.extract_project(extracted.values_mut(), header)?;
        self.extract_user(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "project_observations")
    }

    fn extract_project_admins<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for (id, obj) in extract_objects(&mut result, "admins")? {
                extracted.insert(id, obj);
            }
        }

        self.save_extracted(extracted, header, "project_admins")
    }

    fn extract_project<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for result in results {
            if let Some((id, obj)) = extract_object(result, "project")? {
                extracted.insert(id, obj);
            }
        }

        self.extract_project_admins(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "projects")
    }

    fn extract_observation_field_values<'a, T>(
        &self,
        results: T,
        header: &YamlMapping,
    ) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for (id, obj) in extract_objects(&mut result, "ofvs")? {
                extracted.insert(id, obj);
            }
        }

        self.extract_observation_field(extracted.values_mut(), header)?;
        self.extract_user(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "observation_field_values")
    }

    fn extract_observation_field<'a, T>(
        &self,
        results: T,
        header: &YamlMapping,
    ) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for result in results {
            if let Some((id, obj)) = extract_object(result, "observation_field")? {
                extracted.insert(id, obj);
            }
        }

        self.save_extracted(extracted, header, "observation_fields")
    }

    fn extract_quality_metrics<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for (id, obj) in extract_objects(&mut result, "quality_metrics")? {
                extracted.insert(id, obj);
            }
        }

        self.extract_user(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "quality_metrics")
    }

    fn extract_votes<'a, T>(&self, results: T, header: &YamlMapping) -> Result<(), Error>
    where
        T: IntoIterator<Item = &'a mut JsonMap<String, JsonValue>>,
    {
        let mut extracted = HashMap::new();
        for mut result in results {
            for (id, obj) in extract_objects(&mut result, "votes")? {
                extracted.insert(id, obj);
            }
        }

        self.extract_user(extracted.values_mut(), header)?;

        self.save_extracted(extracted, header, "votes")
    }

    fn save_extracted(
        &self,
        extracted: HashMap<u64, JsonMap<String, JsonValue>>,
        header: &YamlMapping,
        subdir: &str,
    ) -> Result<(), Error> {
        for (id, data) in extracted {
            write_cache(
                &self.path(subdir).join(format!("{}.yaml", id)),
                &header,
                &data,
            )?;
        }

        Ok(())
    }
}

fn extract_object(
    data: &mut JsonMap<String, JsonValue>,
    key: &str,
) -> Result<Option<(u64, JsonMap<String, JsonValue>)>, Error> {
    Ok(match data.get(key) {
        Some(val) => {
            let obj = val
                .as_object()
                .ok_or(internal(&format!("{}: not an object", key)))?
                .clone();
            let id = extract_id(&obj)?;
            data.insert(key.to_string(), id.into());
            data.remove(&format!("{}_id", key));
            Some((id, obj))
        }
        _ => None,
    })
}

fn extract_objects(
    data: &mut JsonMap<String, JsonValue>,
    key: &str,
) -> Result<Vec<(u64, JsonMap<String, JsonValue>)>, Error> {
    Ok(match data.get(key) {
        Some(val) => {
            let arr: Vec<_> = val
                .as_array()
                .ok_or(internal(&format!("{}: not an array", key)))?
                .clone()
                .into_iter()
                .map(|item| {
                    item.as_object()
                        .ok_or(internal(&format!("{} item: not an object", key)))
                        .and_then(|obj| extract_id(&obj).and_then(|id| Ok((id, obj.clone()))))
                })
                .collect::<Result<_, _>>()?;
            let ids: Vec<_> = arr.iter().map(|(id, _)| id).copied().collect();
            data.insert(key.to_string(), ids.into());
            data.remove(&format!("{}_ids", key));
            arr
        }
        _ => vec![],
    })
}
