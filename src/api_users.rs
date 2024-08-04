use std::{
    fs::{read_dir, read_link, remove_file, symlink_metadata},
    io::Error as IoError,
    os::unix::fs::symlink,
    path::Path,
};

use httpdate::fmt_http_date;
use reqwest::header::{IF_MODIFIED_SINCE, IF_NONE_MATCH};

use crate::api::{
    extract_id, extract_single_value, fetch, lookup_cache_id, write_cache, Api, ApiResults,
    CacheHeader,
};
use crate::error::{internal, Error};

impl Api {
    pub(crate) async fn sync_user(&self, username: &str) -> Result<u64, Error> {
        let cached = lookup_cache_id(&self.path("users").join(format!("{}.yaml", username)))?;
        let cached_id = cached.as_ref().and_then(|c| Some(c.id));
        let user = match self
            .fetch_user(cached.and_then(|c| Some(c.header)), username)
            .await?
        {
            Some(user) => user,
            // If nothing was returned, it was a cache hit, no need to update.
            _ => return Ok(cached_id.ok_or(internal("user cache missing id"))?),
        };

        let body = user.body.get(0).ok_or(internal("no user returned"))?;
        let id = extract_id(body)?;
        let login = body
            .get("login")
            .ok_or(internal("user login not found"))?
            .as_str()
            .ok_or(internal("user login was not a string"))?
            .to_string();

        let cache_path = self.path("users").join(format!("{}.yaml", id));
        write_cache(
            &cache_path,
            &user.header,
            &user.body.first().ok_or(internal("user has no body"))?,
        )?;

        self.symlink_user(&login, &id)?;

        Ok(id)
    }

    async fn fetch_user(
        &self,
        cache: Option<CacheHeader>,
        username: &str,
    ) -> Result<Option<ApiResults>, Error> {
        let url = self.endpoint(&format!("/users/{}", username));
        let mut req = self.client.get(url);
        if let Some(cache) = cache {
            req = req.header(IF_MODIFIED_SINCE, fmt_http_date(cache.date.into()));
            if let Some(etag) = cache.etag {
                req = req.header(IF_NONE_MATCH, etag);
            }
        }

        match fetch(req).await? {
            Some((header, res)) => Ok(Some(ApiResults {
                header,
                body: vec![extract_single_value(res)?],
            })),
            _ => Ok(None),
        }
    }

    fn symlink_user(&self, username: &str, id: &u64) -> Result<(), IoError> {
        let dir = self.path("users");
        let link = format!("{}.yaml", username).to_owned();
        let target = &format!("{}.yaml", id);
        let mut exists = false;

        for entry in read_dir(&dir)? {
            let path = entry?.path();
            if let Ok(md) = symlink_metadata(&path) {
                if md.file_type().is_symlink() {
                    if let Ok(target_path) = read_link(&path) {
                        let target = Path::new(target);
                        let ok = path.file_name() == Some(target.as_os_str());
                        if target_path == target && !ok {
                            remove_file(&path)?;
                        }
                        exists |= ok;
                    }
                }
            }
        }

        if !exists {
            symlink(target, dir.join(link))?;
        }

        Ok(())
    }
}
