use std::{collections::HashMap, path::PathBuf};

use serde_json::{Map as JsonMap, Value as JsonValue};
use serde_yaml::Mapping as YamlMapping;

use crate::api::{extract_id, write_cache};
use crate::error::{internal, Error};

pub(crate) struct Normaliser {
    header: YamlMapping,
    data_dir: PathBuf,
    cache: AllTables,
}

macro_rules! all_tables {
    ($($field:ident),*) => {
        struct AllTables {
            $(
                $field:  HashMap<u64, JsonMap<String, JsonValue>>,
            )*
        }

        impl Normaliser {
            fn write_all(&self) -> Result<(), Error> {
                $(
                    self.write_cache(&self.cache.$field, stringify!($field))?;
                )*

                Ok(())
            }
        }

        impl AllTables {
            fn new() -> Self {
                Self {
                    $(
                        $field: HashMap::new(),
                    )*
                }
            }
        }
    };
}

all_tables!(
    applications,
    comments,
    conservation_statuses,
    controlled_term_labels,
    controlled_terms,
    faves,
    flags,
    identifications,
    observation_field_values,
    observation_fields,
    observation_photos,
    observations,
    photos,
    project_admins,
    project_observations,
    projects,
    quality_metrics,
    taxa,
    taxon_changes,
    users,
    votes
);

macro_rules! extract_flags {
    ($self:ident, $($from:ident),*) => {
        $(
            for item in $self.cache.$from.values_mut() {
                for (id, obj) in extract_objects(item, "flags")? {
                    $self.cache.flags.insert(id, obj);
                }
            }
        )*
    };
}

macro_rules! extract_users {
    ($self:ident, $($from:ident),*) => {
        $(
            for item in $self.cache.$from.values_mut() {
                if let Some((id, obj)) = extract_object(item, "user")? {
                    $self.cache.users.insert(id, obj);
                }
            }
        )*
    };
}

impl Normaliser {
    pub(crate) fn new(
        header: YamlMapping,
        observations: HashMap<u64, JsonMap<String, JsonValue>>,
        data_dir: &PathBuf,
    ) -> Self {
        let mut cache = AllTables::new();
        cache.observations = observations;
        Self {
            header,
            data_dir: data_dir.to_path_buf(),
            cache,
        }
    }

    pub(crate) fn write(&mut self) -> Result<(), Error> {
        // NEEDS: observations
        self.extract_annotations()?;
        self.extract_applications()?;
        self.extract_comments()?;
        self.extract_faves()?;
        self.extract_identifications()?;
        self.extract_observation_field_values()?;
        self.extract_observation_photos()?;
        self.extract_project_observations()?;
        self.extract_quality_metrics()?;
        self.extract_votes()?;

        // NEEDS: annotations
        self.extract_labels()?;

        // NEEDS: identifications
        self.extract_taxa()?;
        self.extract_taxon_changes()?;

        // NEEDS: observation_field_values
        self.extract_observation_field()?;

        // NEEDS: project_observations
        self.extract_projects()?;

        // NEEDS: projects
        self.extract_project_admins()?;

        // NEEDS: taxa
        self.extract_conservation_status()?;

        // NEEDS: comments, identifications, observations, photos, projects
        self.extract_flags()?;

        // NEEDS: observation_photos, taxa
        self.extract_photos()?;

        // NEEDS: many other fields, should be the last
        self.extract_users()?;

        self.write_all()
    }

    fn extract_annotations(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            if let Some(annotations) = obs.get_mut("annotations") {
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
                                self.cache.controlled_terms.insert(id, obj);
                            }

                            self.cache.controlled_terms.insert(id, obj);
                        }
                    }

                    // TODO: This clone causes the user to not be extracted!!!
                    //self.extract_user(annotation, header)?;
                    //self.extract_votes([&annotation], header)?;
                }
            }
        }

        Ok(())
    }

    fn extract_applications(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            if let Some((id, obj)) = extract_object(obs, "application")? {
                self.cache.applications.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_comments(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(obs, "comments")? {
                self.cache.comments.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_taxa(&mut self) -> Result<(), Error> {
        for mut obs in self.cache.observations.values_mut() {
            for key in ["taxon", "community_taxon"] {
                if let Some((id, obj)) = extract_object(&mut obs, key)? {
                    self.cache.taxa.insert(id, obj);
                }
            }
        }

        for mut ident in self.cache.identifications.values_mut() {
            for key in ["taxon", "previous_observation_taxon"] {
                if let Some((id, obj)) = extract_object(&mut ident, key)? {
                    self.cache.taxa.insert(id, obj);
                }
            }
        }

        // Ancestors are self-references, create a copy first.
        // TODO: make sure not to overwrite with less detailed values.
        let mut ancestors = HashMap::new();
        for taxon in self.cache.taxa.values_mut() {
            for (id, obj) in extract_objects(taxon, "ancestors")? {
                ancestors.insert(id, obj);
            }
        }

        self.cache.taxa.extend(ancestors);

        Ok(())
    }

    fn extract_taxon_changes(&mut self) -> Result<(), Error> {
        for mut ident in self.cache.identifications.values_mut() {
            if let Some((id, obj)) = extract_object(&mut ident, "taxon_change")? {
                self.cache.taxon_changes.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_users(&mut self) -> Result<(), Error> {
        extract_users!(
            self,
            comments,
            faves,
            identifications,
            observation_field_values,
            observations,
            project_observations,
            quality_metrics,
            votes
        );

        Ok(())
    }

    fn extract_labels(&mut self) -> Result<(), Error> {
        for result in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(result, "labels")? {
                self.cache.controlled_term_labels.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_conservation_status(&mut self) -> Result<(), Error> {
        for taxon in self.cache.taxa.values_mut() {
            if let Some((id, obj)) = extract_object(taxon, "conservation_status")? {
                self.cache.conservation_statuses.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_faves(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(obs, "faves")? {
                self.cache.faves.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_flags(&mut self) -> Result<(), Error> {
        extract_flags!(
            self,
            comments,
            identifications,
            observations,
            photos,
            projects
        );

        Ok(())
    }

    fn extract_identifications(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            for key in ["identifications", "non_owner_ids"] {
                for (id, obj) in extract_objects(obs, key)? {
                    self.cache.identifications.insert(id, obj);
                }
            }
        }

        Ok(())
    }

    fn extract_photos(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(obs, "photos")? {
                self.cache.photos.insert(id, obj);
            }
        }

        for obs_photo in self.cache.observation_photos.values_mut() {
            if let Some((id, obj)) = extract_object(obs_photo, "photo")? {
                self.cache.photos.insert(id, obj);
            }
        }

        for taxon in self.cache.taxa.values_mut() {
            if let Some((id, obj)) = extract_object(taxon, "default_photo")? {
                self.cache.photos.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_observation_photos(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(obs, "observation_photos")? {
                self.cache.observation_photos.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_project_observations(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(obs, "project_observations")? {
                self.cache.project_observations.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_project_admins(&mut self) -> Result<(), Error> {
        for proj in self.cache.projects.values_mut() {
            for (id, obj) in extract_objects(proj, "admins")? {
                self.cache.project_admins.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_projects(&mut self) -> Result<(), Error> {
        for project_obs in self.cache.project_observations.values_mut() {
            if let Some((id, obj)) = extract_object(project_obs, "project")? {
                self.cache.projects.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_observation_field_values(&mut self) -> Result<(), Error> {
        for obs in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(obs, "ofvs")? {
                self.cache.observation_field_values.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_observation_field(&mut self) -> Result<(), Error> {
        for ofv in self.cache.observation_field_values.values_mut() {
            if let Some((id, obj)) = extract_object(ofv, "observation_field")? {
                self.cache.observation_fields.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_quality_metrics(&mut self) -> Result<(), Error> {
        for mut obs in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(&mut obs, "quality_metrics")? {
                self.cache.quality_metrics.insert(id, obj);
            }
        }

        Ok(())
    }

    fn extract_votes(&mut self) -> Result<(), Error> {
        for mut obs in self.cache.observations.values_mut() {
            for (id, obj) in extract_objects(&mut obs, "votes")? {
                self.cache.votes.insert(id, obj);
            }
        }

        Ok(())
    }

    fn write_cache(
        &self,
        extracted: &HashMap<u64, JsonMap<String, JsonValue>>,
        subdir: &str,
    ) -> Result<(), Error> {
        for (id, data) in extracted {
            write_cache(
                &self.data_dir.join(subdir).join(format!("{}.yaml", id)),
                &self.header,
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
            if val.is_null() {
                None
            } else {
                let obj = val
                    .as_object()
                    .ok_or(internal(&format!("{}: not an object", key)))?
                    .clone();
                let id = extract_id(&obj)?;
                data.insert(key.to_string(), id.into());
                data.remove(&format!("{}_id", key));
                Some((id, obj))
            }
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
