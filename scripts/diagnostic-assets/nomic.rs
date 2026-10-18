use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
#[cfg(feature = "test-support")]
use std::time::Duration;
use std::time::Instant;
use std::{
    fs as file_system, io,
    path::{Path, PathBuf},
};

use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::nomic_bert::{
    Config as NomicBertConfig, NomicBertModel, l2_normalize as candle_l2_normalize, mean_pooling,
};
use safetensors::{Dtype as SafeTensorDType, SafeTensors};
use tokenizers::{Tokenizer, TruncationDirection};

use crate::embedder::load_observer::{EmbedderLoadEvent, EmbedderLoadObserver};
use crate::embedder::{
    Embedder, EmbedderConfig, EmbedderRegistration, EmbeddingInput, EmbeddingOutput,
    EventSearchCapability, EventSearchInference, EventSearchRole, EventSearchSeal,
    PreparedEventSearchDocument, PreparedEventSearchQuery,
};
use crate::embedding_space::{BuiltinSpaceRegistry, event_history_embedding_space};
use crate::{
    CgError, DistanceMetric, EmbeddingSpace, EmbeddingSpaceBinding, EvidenceKind, EvidenceProducer,
    ModelSource, Result,
};

const NOMIC_TEXT_DEFAULT_ID: &str = "text_default_nomic_v1_5_768";
const NOMIC_MODEL_ID: &str = "nomic-embed-text-v1.5";
const NOMIC_DEFAULT_DIMENSION: usize = 768;
const NOMIC_MODEL_DOWNLOAD_LIMIT_BYTES: u64 = 600 * 1024 * 1024;
// These limits deliberately remain separate even while their values match. Plain embedding keeps
// its established model-window truncation, while event-search preparation reports and refuses by
// its own exact ceiling; changing one contract must never silently change the other.
const NOMIC_PLAIN_EMBED_MAX_SEQUENCE_TOKENS: usize = 8192;
const NOMIC_EVENT_SEARCH_MAX_SEQUENCE_TOKENS: usize = 8192;
/// The one system-added task instruction Nomic expects in front of a passage that is being stored
/// for later retrieval. It is added outside the caller's content, exactly once, and the caller's
/// own text is never inspected for already beginning with these words.
const EVENT_SEARCH_DOCUMENT_PREFIX: &str = "search_document: ";
/// The query-side counterpart of [`EVENT_SEARCH_DOCUMENT_PREFIX`].
const EVENT_SEARCH_QUERY_PREFIX: &str = "search_query: ";
const CG_MODEL_CACHE_ROOT_ENV: &str = "CG_MODEL_CACHE_ROOT";
/// Test-support-only directory: a prepared event-search embedding creates the reached marker and
/// waits for the release marker. It is absent from ordinary builds.
#[cfg(feature = "test-support")]
const NOMIC_PREPARED_EMBEDDING_RENDEZVOUS_ENV: &str = "CG_NOMIC_TEST_PREPARED_EMBEDDING_BARRIER";

const NOMIC_REQUIRED_TENSORS: &[(&str, &[usize])] = &[
    ("embeddings.word_embeddings.weight", &[30_528, 768]),
    ("embeddings.token_type_embeddings.weight", &[2, 768]),
    ("emb_ln.weight", &[768]),
    ("emb_ln.bias", &[768]),
    ("encoder.layers.0.attn.Wqkv.weight", &[2_304, 768]),
    ("encoder.layers.0.attn.out_proj.weight", &[768, 768]),
    ("encoder.layers.0.mlp.fc11.weight", &[3_072, 768]),
    ("encoder.layers.0.mlp.fc12.weight", &[3_072, 768]),
    ("encoder.layers.0.mlp.fc2.weight", &[768, 3_072]),
    ("encoder.layers.0.norm1.weight", &[768]),
    ("encoder.layers.0.norm2.weight", &[768]),
    ("encoder.layers.11.attn.Wqkv.weight", &[2_304, 768]),
    ("encoder.layers.11.norm2.weight", &[768]),
];

static NOMIC_MODEL_CACHE: OnceLock<Mutex<HashMap<NomicModelCacheKey, Arc<LoadedNomicModel>>>> =
    OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NomicModelCacheKey {
    model_dir: PathBuf,
    pinned_revision: Option<String>,
    installed_revision: Option<String>,
}

#[derive(Clone)]
pub struct NomicTextEmbedder {
    config: EmbedderConfig,
    dimension: usize,
    load_observer: Option<Arc<dyn EmbedderLoadObserver>>,
    loader_events: Arc<Mutex<Vec<EmbedderLoadEvent>>>,
    /// What the artifact this embedder resolves to is, once there are files to judge: the verified
    /// canonical event-history artifact, or something installed that is not canonical. Decided
    /// once, on first use over a present installation, and shared by every clone of this embedder
    /// — config/tokenizer parsing is remembered; publication state and revision are still checked
    /// under the cache lock before reusing a canonical verdict. Neither check reads weights.
    /// An installation that is simply
    /// not there yet is deliberately NOT remembered: see
    /// [`NomicTextEmbedder::event_history_artifact`].
    canonical_event_history_artifact: Arc<OnceLock<EventHistoryArtifact>>,
}

impl std::fmt::Debug for NomicTextEmbedder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NomicTextEmbedder")
            .field("config", &self.config)
            .field("dimension", &self.dimension)
            .field("has_load_observer", &self.load_observer.is_some())
            .finish()
    }
}

impl NomicTextEmbedder {
    /// The bundled event-history encoding's declared prepared-token ceiling. This is pure model
    /// metadata: it does not inspect the cache, initialize an embedder, load weights, or prepare
    /// source text. Callers may use it to reject an incoherent requested passage profile before
    /// deferred capture writes anything.
    pub const fn declared_event_search_capability() -> EventSearchCapability {
        match EventSearchCapability::new(NOMIC_EVENT_SEARCH_MAX_SEQUENCE_TOKENS) {
            Some(capability) => capability,
            None => panic!("the bundled event-search ceiling is non-zero"),
        }
    }

    pub fn new(config: EmbedderConfig) -> Self {
        Self::new_for_dimension(config, NOMIC_DEFAULT_DIMENSION)
    }

    pub fn new_for_dimension(config: EmbedderConfig, dimension: usize) -> Self {
        Self {
            config,
            dimension,
            load_observer: None,
            loader_events: Arc::new(Mutex::new(Vec::new())),
            canonical_event_history_artifact: Arc::new(OnceLock::new()),
        }
    }

    pub fn new_with_load_observer(
        config: EmbedderConfig,
        load_observer: Arc<dyn EmbedderLoadObserver>,
    ) -> Self {
        Self {
            config,
            dimension: NOMIC_DEFAULT_DIMENSION,
            load_observer: Some(load_observer),
            loader_events: Arc::new(Mutex::new(Vec::new())),
            canonical_event_history_artifact: Arc::new(OnceLock::new()),
        }
    }

    pub fn structured_loader_events(&self) -> Vec<EmbedderLoadEvent> {
        self.loader_events
            .lock()
            .map(|events| events.clone())
            .unwrap_or_default()
    }

    fn observe(&self, event: EmbedderLoadEvent) {
        if let Ok(mut events) = self.loader_events.lock() {
            events.push(event.clone());
        }
        if let Some(observer) = &self.load_observer {
            observer.observe(event);
        }
    }

    fn load_model(&self) -> Result<Arc<LoadedNomicModel>> {
        self.load_model_with_fetch_policy(FetchPolicy::MayInstall)
    }

    /// Load whatever is already installed, and never install anything.
    ///
    /// A caller that asked to prepare or to embed a value it already holds asked for neither a
    /// download nor consent to one, so a missing artifact is reported as
    /// [`CgError::EmbedderModelNotAvailable`] naming the resolved directory and the fetch branch is
    /// never entered — the same answer an offline configuration gives, on any configuration.
    fn load_model_without_installing(&self) -> Result<Arc<LoadedNomicModel>> {
        self.load_model_with_fetch_policy(FetchPolicy::NeverInstall)
    }

    fn load_model_with_fetch_policy(&self, fetch: FetchPolicy) -> Result<Arc<LoadedNomicModel>> {
        if matches!(self.config.source, ModelSource::Disabled) {
            return Err(CgError::TextEmbedderDisabled);
        }
        let model_dir = resolve_model_dir(&self.config)?;
        // Keep the cache directory stable from revision checking through all model reads.
        // The catalog's mutating operations use the same persistent .lock inode.
        if !(model_dir
            .try_exists()
            .map_err(|err| cache_io_error(&model_dir, err))?
            || (matches!(fetch, FetchPolicy::MayInstall) && config_can_fetch(&self.config)))
        {
            return Err(CgError::EmbedderModelNotAvailable {
                source: source_name(&self.config),
                path: model_dir.display().to_string(),
            });
        }
        NomicCacheAccess::with(&model_dir, |access| {
            let mut cache_key = NomicModelCacheKey {
                model_dir: model_dir.clone(),
                pinned_revision: self.config.pinned_revision.clone(),
                installed_revision: installed_revision(&model_dir)?,
            };
            let cache = NOMIC_MODEL_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
            let mut guard = cache
                .lock()
                .map_err(|_| CgError::Engine("nomic model cache lock poisoned".to_string()))?;
            if let Some(model) = guard.get(&cache_key) {
                let loaded = Arc::clone(model);
                let revision_path = model_dir.join("REVISION");
                check_revision_marker(&revision_path, self.config.pinned_revision.as_deref())?;
                self.observe(EmbedderLoadEvent::CacheHit {
                    model_id: NOMIC_MODEL_ID.to_string(),
                    destination: model_dir.clone(),
                });
                return Ok(loaded);
            }

            self.ensure_cache_ready(&model_dir, fetch, access)?;
            // A successful install may have changed the revision since the lookup above.
            // Unpinned warm readers must not reuse a different installed revision's model.
            cache_key.installed_revision = installed_revision(&model_dir)?;
            self.observe(EmbedderLoadEvent::LoadStarted {
                model_id: NOMIC_MODEL_ID.to_string(),
                byte_size: None,
                destination: model_dir.clone(),
            });
            let (artifacts, elapsed) = timed(|| load_nomic_artifacts(&model_dir));
            let loaded = Arc::new(artifacts?);
            self.observe(EmbedderLoadEvent::LoadCompleted {
                model_id: NOMIC_MODEL_ID.to_string(),
                byte_size: file_system::metadata(model_dir.join("model.safetensors"))
                    .map(|metadata| metadata.len())
                    .ok(),
                destination: model_dir.clone(),
                elapsed,
            });
            guard.insert(cache_key, Arc::clone(&loaded));
            Ok(loaded)
        })
    }

    fn ensure_cache_ready(
        &self,
        model_dir: &Path,
        fetch: FetchPolicy,
        access: &NomicCacheAccess,
    ) -> Result<()> {
        if matches!(self.config.source, ModelSource::Disabled) {
            return Ok(());
        }
        let may_install =
            matches!(fetch, FetchPolicy::MayInstall) && config_can_fetch(&self.config);

        let model_path = model_dir.join("model.safetensors");
        let tokenizer_path = model_dir.join("tokenizer.json");
        let config_path = model_dir.join("config.json");
        let revision_path = model_dir.join("REVISION");
        let all_present = cache_file_present(&model_path)?
            && cache_file_present(&tokenizer_path)?
            && cache_file_present(&config_path)?;
        if all_present {
            match verify_installed_artifact(
                &model_path,
                &tokenizer_path,
                &config_path,
                &revision_path,
                self.config.pinned_revision.as_deref(),
            ) {
                Ok(()) => return Ok(()),
                Err(CgError::EmbedderModelRevisionMismatch { .. }) if may_install => {}
                Err(err) => return Err(err),
            }
        }
        if !may_install {
            return Err(CgError::EmbedderModelNotAvailable {
                source: source_name(&self.config),
                path: model_dir.display().to_string(),
            });
        }

        self.observe(EmbedderLoadEvent::FetchStarted {
            model_id: NOMIC_MODEL_ID.to_string(),
            byte_size: None,
            destination: model_dir.to_path_buf(),
        });
        eprintln!(
            "context-graph embedding fetch started model={} path={}",
            NOMIC_MODEL_ID,
            model_dir.display()
        );
        let (fetched, elapsed) = timed(|| -> Result<()> {
            file_system::create_dir_all(model_dir).map_err(|err| {
                CgError::Engine(format!(
                    "nomic cache directory create failed at {}: {err}",
                    model_dir.display()
                ))
            })?;
            let staging = NomicDownloadDirectory::new(model_dir)?;
            let staged = (|| -> Result<()> {
                let repo = match &self.config.source {
                    ModelSource::HuggingFace { repo } => repo.as_str(),
                    _ => unreachable!("source checked above"),
                };
                let revision = self.config.pinned_revision.as_deref().unwrap_or("main");
                download_hf_file(
                    repo,
                    revision,
                    "model.safetensors",
                    &staging.0.join("model.safetensors"),
                    NOMIC_MODEL_DOWNLOAD_LIMIT_BYTES,
                )?;
                download_hf_file(
                    repo,
                    revision,
                    "tokenizer.json",
                    &staging.0.join("tokenizer.json"),
                    64 * 1024 * 1024,
                )?;
                download_hf_file(
                    repo,
                    revision,
                    "config.json",
                    &staging.0.join("config.json"),
                    1024 * 1024,
                )?;
                file_system::write(staging.0.join("REVISION"), revision).map_err(|err| {
                    CgError::Engine(format!(
                        "nomic cache revision write failed at {}: {err}",
                        revision_path.display()
                    ))
                })?;
                publish_download(
                    access,
                    &staging.0,
                    model_dir,
                    self.config.pinned_revision.as_deref(),
                )
            })();
            staged.map_err(|error| artifact_error_at_destination(error, &staging.0, model_dir))
        });
        fetched?;
        let model_byte_size = file_system::metadata(&model_path)
            .map(|metadata| metadata.len())
            .ok();
        self.observe(EmbedderLoadEvent::FetchCompleted {
            model_id: NOMIC_MODEL_ID.to_string(),
            byte_size: model_byte_size,
            destination: model_dir.to_path_buf(),
            elapsed,
        });
        eprintln!(
            "context-graph embedding fetch completed model={} path={}",
            NOMIC_MODEL_ID,
            model_dir.display()
        );
        Ok(())
    }

    fn forward(&self, loaded: &LoadedNomicModel, text: &str) -> Result<Vec<f32>> {
        let mut encoding =
            loaded
                .tokenizer
                .encode(text, true)
                .map_err(|_| CgError::EmbedderModelCorrupt {
                    path: loaded.tokenizer_path.clone(),
                })?;
        encoding.truncate(
            NOMIC_PLAIN_EMBED_MAX_SEQUENCE_TOKENS,
            0,
            TruncationDirection::Right,
        );
        self.forward_token_ids(loaded, encoding.get_ids(), encoding.get_attention_mask())
    }

    /// The one inference path: token ids and their attention mask in, a normalized vector out.
    /// Plain text embedding reaches it through [`NomicTextEmbedder::forward`] after tokenizing,
    /// and event-search embedding reaches it with the token ids its preparation already produced,
    /// so both roles run the identical forward pass, pooling and normalization.
    fn forward_token_ids(
        &self,
        loaded: &LoadedNomicModel,
        ids: &[u32],
        attention_mask_values: &[u32],
    ) -> Result<Vec<f32>> {
        let input_ids = Tensor::new(ids, &loaded.device)
            .map_err(candle_err)?
            .unsqueeze(0)
            .map_err(candle_err)?;
        let attention_mask = Tensor::new(attention_mask_values, &loaded.device)
            .map_err(candle_err)?
            .unsqueeze(0)
            .map_err(candle_err)?;
        let hidden = loaded
            .model
            .forward(&input_ids, None, Some(&attention_mask))
            .map_err(candle_err)?;
        let pooled = mean_pooling(&hidden, &attention_mask).map_err(candle_err)?;
        let normalized = candle_l2_normalize(&pooled).map_err(candle_err)?;
        let values = normalized.to_vec2::<f32>().map_err(candle_err)?;
        let full = values
            .first()
            .ok_or_else(|| CgError::Engine("nomic model returned empty batch".to_string()))?;
        if self.dimension > full.len() {
            return Err(CgError::EmbeddingDimensionMismatch {
                space_id: text_space_id_for_dimension(self.dimension),
                expected: full.len() as u32,
                found: self.dimension as u32,
            });
        }
        let mut vector = full[..self.dimension].to_vec();
        l2_normalize(&mut vector);
        Ok(vector)
    }

    /// The batched inference path: several token-id sequences right-padded to the longest, with
    /// an attention mask that zeroes the padding, through one forward pass. Masked mean pooling
    /// keeps every row's vector equal to what [`NomicTextEmbedder::forward_token_ids`] answers for
    /// that sequence alone.
    fn forward_token_id_batch(
        &self,
        loaded: &LoadedNomicModel,
        sequences: &[&[u32]],
    ) -> Result<Vec<Vec<f32>>> {
        let width = sequences.iter().map(|ids| ids.len()).max().unwrap_or(0);
        let mut ids = Vec::with_capacity(sequences.len() * width);
        let mut mask = Vec::with_capacity(sequences.len() * width);
        for sequence in sequences {
            ids.extend_from_slice(sequence);
            ids.resize(ids.len() + width - sequence.len(), 0);
            mask.resize(mask.len() + sequence.len(), 1u32);
            mask.resize(mask.len() + width - sequence.len(), 0u32);
        }
        let shape = (sequences.len(), width);
        let input_ids = Tensor::from_vec(ids, shape, &loaded.device).map_err(candle_err)?;
        let attention_mask = Tensor::from_vec(mask, shape, &loaded.device).map_err(candle_err)?;
        let hidden = loaded
            .model
            .forward(&input_ids, None, Some(&attention_mask))
            .map_err(candle_err)?;
        let pooled = mean_pooling(&hidden, &attention_mask).map_err(candle_err)?;
        let normalized = candle_l2_normalize(&pooled).map_err(candle_err)?;
        let rows = normalized.to_vec2::<f32>().map_err(candle_err)?;
        if rows.len() != sequences.len() {
            return Err(CgError::Engine(format!(
                "nomic model returned {} rows for a batch of {}",
                rows.len(),
                sequences.len()
            )));
        }
        rows.into_iter()
            .map(|full| {
                if self.dimension > full.len() {
                    return Err(CgError::EmbeddingDimensionMismatch {
                        space_id: text_space_id_for_dimension(self.dimension),
                        expected: full.len() as u32,
                        found: self.dimension as u32,
                    });
                }
                let mut vector = full[..self.dimension].to_vec();
                l2_normalize(&mut vector);
                Ok(vector)
            })
            .collect()
    }

    /// What this embedder's artifact is, as far as the canonical event-history encoding is
    /// concerned — the exact admission [`EventHistoryTextEmbedder::over`] applies at registration,
    /// shared with raw role-aware preparation so both judge the reserved identity on the same
    /// evidence.
    ///
    /// A verdict reached on files that are actually there — canonical, or installed and not
    /// canonical — is decided once and remembered for the life of the embedder and every clone of
    /// it: that check reads the revision marker, the model config and the tokenizer, never the
    /// weights. Config/tokenizer parsing happens once; repeated admission still locks the cache,
    /// rejects pending publications and checks that the installed revision has not changed.
    /// The weights are still read only by the loader.
    ///
    /// "Nothing installed" is NOT a verdict about an artifact and is never remembered. A handle
    /// held by a running application that installs the model on first use would otherwise keep
    /// answering "not installed" for the rest of its life; instead it re-looks each time, which
    /// costs a presence check on three file names and no read of the model at all. The re-look is
    /// what puts the canonical admission back in front of the load path, so an installation that
    /// appears later is admitted only if it is the canonical artifact.
    fn event_history_artifact(&self) -> EventHistoryArtifact {
        if let Some(decided) = self.canonical_event_history_artifact.get() {
            if *decided == EventHistoryArtifact::Canonical {
                let Ok(model_dir) = resolve_model_dir(&self.config) else {
                    return EventHistoryArtifact::Noncanonical;
                };
                if !model_dir.exists() {
                    return EventHistoryArtifact::Absent;
                }
                let Ok(_access) = NomicCacheAccess::acquire(&model_dir) else {
                    return EventHistoryArtifact::Noncanonical;
                };
                // Cache expensive canonical parsing, never admission of a pending publisher
                // or a different revision installed since that parsing took place.
                if check_revision_marker(
                    &model_dir.join("REVISION"),
                    Some(crate::embedder::NOMIC_DEFAULT_REVISION),
                )
                .is_err()
                {
                    return EventHistoryArtifact::Noncanonical;
                }
            }
            return *decided;
        }
        let observed = self.observe_event_history_artifact();
        if observed != EventHistoryArtifact::Absent {
            let _ = self.canonical_event_history_artifact.set(observed);
        }
        observed
    }

    /// Read the installation as it stands right now. Presence first, then the canonical admission
    /// on whatever is present; never the weights.
    fn observe_event_history_artifact(&self) -> EventHistoryArtifact {
        if matches!(self.config.source, ModelSource::Disabled) {
            return EventHistoryArtifact::Noncanonical;
        }
        let Ok(model_dir) = resolve_model_dir(&self.config) else {
            return EventHistoryArtifact::Noncanonical;
        };
        if !model_dir.exists() {
            return EventHistoryArtifact::Absent;
        }
        let Ok(_access) = NomicCacheAccess::acquire(&model_dir) else {
            return EventHistoryArtifact::Noncanonical;
        };
        if !event_history_artifact_files_installed(&model_dir) {
            return EventHistoryArtifact::Absent;
        }
        if verify_local_event_history_artifact(&model_dir).is_ok() {
            EventHistoryArtifact::Canonical
        } else {
            EventHistoryArtifact::Noncanonical
        }
    }

    /// Whether role-aware event-search preparation may proceed on this embedder's artifact.
    ///
    /// An ABSENT artifact is admitted deliberately, whatever the configuration's fetch policy:
    /// nothing is installed to judge, so the role methods go on to the loader and the caller gets
    /// the same typed [`CgError::EmbedderModelNotAvailable`] naming the exact missing directory
    /// that plain [`Embedder::embed`] already gives — never a "the role is unsupported" answer
    /// that hides a model the user simply has not installed yet. That state is re-read on
    /// every call rather than remembered, so a model installed after this handle was made is picked
    /// up by the same handle. Support is about what kind of embedder this is; whether the artifact
    /// is installed is a separate answer, which is why a download-enabled configuration with
    /// nothing installed declares the capability too. Preparation still can never turn into a
    /// fetch: the role methods load under [`FetchPolicy::NeverInstall`], so the missing artifact is
    /// reported instead of downloaded. Weights that ARE installed but are not the canonical
    /// artifact stay unsupported: they are a different encoding, and stamping them with the
    /// reserved identity is what would make stored vectors silently incomparable.
    fn artifact_admits_event_search(&self) -> bool {
        match self.event_history_artifact() {
            EventHistoryArtifact::Canonical => true,
            EventHistoryArtifact::Absent => true,
            EventHistoryArtifact::Noncanonical => false,
        }
    }

    /// Refuses role-aware event-search preparation on an embedder that cannot answer for the
    /// dedicated event-history space: the space identifier names 768 dimensions, so a truncated
    /// build of this model never claims it, a disabled text embedder never claims it either, and
    /// installed weights that are not the verified canonical artifact never claim it — same-shaped
    /// weights under an unverified revision are a different encoding, and stamping them with the
    /// reserved identity is what would make stored vectors silently incomparable. See
    /// [`NomicTextEmbedder::artifact_admits_event_search`] for why a model that is not installed
    /// at all is a different answer from one that is installed and wrong.
    fn require_event_search_capability(&self) -> Result<()> {
        if matches!(self.config.source, ModelSource::Disabled) {
            return Err(CgError::TextEmbedderDisabled);
        }
        if self.dimension != NOMIC_DEFAULT_DIMENSION || !self.artifact_admits_event_search() {
            return Err(CgError::EventSearchPreparationUnsupported {
                embedding_space_id: self.embedding_space().embedding_space_id,
            });
        }
        Ok(())
    }

    /// Load the weights for a role method, with the canonical admission applied to the files that
    /// are actually there when the load happens.
    ///
    /// The admission ahead of a load can only judge what is installed at that moment, and a handle
    /// may well have been made before anything was installed — a missing installation is retryable
    /// on the same handle, so that is a normal state, not a dead one. The admission is therefore
    /// applied again once the load has succeeded, when an installation is present by definition, so
    /// a noncanonical one is refused before the role method continues rather than being stamped
    /// with the reserved identity. After the first verdict reached on present files, repeating it
    /// reuses the parsed verdict while checking publication state and revision under the cache
    /// lock. Neither admission pass reads the weights.
    fn load_model_admitted_for_event_search(&self) -> Result<Arc<LoadedNomicModel>> {
        self.require_event_search_capability()?;
        let loaded = self.load_model_without_installing()?;
        self.require_event_search_capability()?;
        Ok(loaded)
    }

    /// Tokenize one system-added task instruction plus the caller's untouched content, and report
    /// the exact model input alongside the byte span each of its tokens occupies in the caller's
    /// ORIGINAL, unprefixed text.
    ///
    /// The prefix's own tokens and the model's special tokens occupy no source bytes and are
    /// reported as a zero-width span, so a consumer reading the map sees exactly the source
    /// coverage and nothing invented. Every returned span lands on a UTF-8 character boundary
    /// because it comes from the selected tokenizer's own offsets, shifted by the prefix's byte
    /// length.
    fn tokenize_event_search_input(
        &self,
        prefix: &str,
        text: &str,
    ) -> Result<TokenizedEventSearchInput> {
        let loaded = self.load_model_admitted_for_event_search()?;
        let model_input = format!("{prefix}{text}");
        #[cfg(all(test, context_graph_internal_test_support))]
        PREPARATION_WORK.with(|work| {
            let (calls, bytes) = work.get();
            work.set((calls + 1, bytes + model_input.len()));
        });
        let encoding = loaded
            .tokenizer
            .encode(model_input.as_str(), true)
            .map_err(|_| CgError::EmbedderModelCorrupt {
                path: loaded.tokenizer_path.clone(),
            })?;
        let prefix_len = prefix.len();
        let special_tokens = encoding.get_special_tokens_mask();
        let offsets = encoding.get_offsets();
        let mut byte_boundaries = Vec::with_capacity(offsets.len());
        for (index, &(start, end)) in offsets.iter().enumerate() {
            let is_special_token = special_tokens.get(index).is_some_and(|flag| *flag == 1);
            if is_special_token || start < prefix_len || end <= prefix_len {
                byte_boundaries.push((0, 0));
            } else {
                byte_boundaries.push((start - prefix_len, end - prefix_len));
            }
        }
        Ok(TokenizedEventSearchInput {
            token_ids: encoding.get_ids().to_vec(),
            byte_boundaries,
        })
    }

    /// Refuses an inference this model's own preparation never authorized.
    ///
    /// A vector stamped with the reserved event-history identity must have been produced from
    /// tokens this bundled model prepared, for the role it is now being asked to run. An inference
    /// is a capability, not a token bag: another embedder can be handed one by its own checked
    /// entrypoint and pass it on to these hooks directly, so the hooks read the encoding the
    /// inference was actually authorized for and refuse anything else — a foreign profile, a
    /// profile that merely names the reserved identity without this library having vouched for it,
    /// or the other role's tokens. The refusal names the reserved profile as the one expected and
    /// the forwarding embedder's own as what arrived.
    fn authorize_inference(
        &self,
        inference: &EventSearchInference,
        hook_role: EventSearchRole,
    ) -> Result<()> {
        let expected = self.encoding_profile_id();
        if inference.authorized_profile_id() != expected
            || !inference.has_canonical_authority()
            || inference.role() != hook_role
        {
            return Err(CgError::EventSearchPreparationProfileMismatch {
                expected_profile: expected,
                actual_profile: inference.authorized_profile_id().to_string(),
            });
        }
        Ok(())
    }

    /// Run the shared inference path over an already-prepared model input and report the vector
    /// under the dedicated event-history space, refusing by exact token count anything the model
    /// window cannot hold. Preparation itself never refuses — a consumer splitting a large unit
    /// needs the full count and the full boundary map to cut it — so this is the one place an
    /// oversized unit is turned away, and it is turned away rather than silently truncated.
    fn embed_prepared_token_ids(
        &self,
        token_count: usize,
        token_ids: &[u32],
    ) -> Result<EmbeddingOutput> {
        self.require_event_search_capability()?;
        if token_count > NOMIC_EVENT_SEARCH_MAX_SEQUENCE_TOKENS {
            return Err(CgError::PreparedInputExceedsModelLimit {
                token_count,
                limit: NOMIC_EVENT_SEARCH_MAX_SEQUENCE_TOKENS,
            });
        }
        let loaded = self.load_model_admitted_for_event_search()?;
        #[cfg(feature = "test-support")]
        pause_prepared_embedding_for_test();
        let attention_mask = vec![1u32; token_ids.len()];
        let vector = self.forward_token_ids(&loaded, token_ids, &attention_mask)?;
        Ok(EmbeddingOutput {
            embedding_space_id: BuiltinSpaceRegistry::EVENT_HISTORY_DOCUMENT_QUERY_ID.to_string(),
            dimension: self.dimension,
            producer: nomic_producer(),
            vector,
        })
    }

    /// [`NomicTextEmbedder::embed_prepared_token_ids`] over several authorized inferences at
    /// once: the same ceiling refusal for each, then one batched forward pass.
    fn embed_prepared_token_id_batch(
        &self,
        inferences: &[EventSearchInference],
    ) -> Result<Vec<EmbeddingOutput>> {
        self.require_event_search_capability()?;
        for inference in inferences {
            if inference.token_count() > NOMIC_EVENT_SEARCH_MAX_SEQUENCE_TOKENS {
                return Err(CgError::PreparedInputExceedsModelLimit {
                    token_count: inference.token_count(),
                    limit: NOMIC_EVENT_SEARCH_MAX_SEQUENCE_TOKENS,
                });
            }
        }
        if inferences.is_empty() {
            return Ok(Vec::new());
        }
        let loaded = self.load_model_admitted_for_event_search()?;
        #[cfg(feature = "test-support")]
        pause_prepared_embedding_for_test();
        let sequences = inferences
            .iter()
            .map(|inference| inference.token_ids())
            .collect::<Vec<_>>();
        Ok(self
            .forward_token_id_batch(&loaded, &sequences)?
            .into_iter()
            .map(|vector| EmbeddingOutput {
                embedding_space_id: BuiltinSpaceRegistry::EVENT_HISTORY_DOCUMENT_QUERY_ID
                    .to_string(),
                dimension: self.dimension,
                producer: nomic_producer(),
                vector,
            })
            .collect())
    }
}

/// One tokenized event-search model input: the token ids the model will see, and the byte span
/// each of them occupies in the caller's original, unprefixed text (zero-width where a token —
/// the task instruction's own, or the model's special tokens — covers no source bytes).
struct TokenizedEventSearchInput {
    token_ids: Vec<u32>,
    byte_boundaries: Vec<(usize, usize)>,
}

fn nomic_producer() -> EvidenceProducer {
    EvidenceProducer {
        system: crate::CG_EDGE_SYSTEM_TAG.to_string(),
        model_name: NOMIC_MODEL_ID.to_string(),
        model_version: "1.5".to_string(),
        pipeline_version: "nomic-bert-candle".to_string(),
    }
}

impl Embedder for NomicTextEmbedder {
    fn embed(&self, input: EmbeddingInput) -> Result<EmbeddingOutput> {
        let EmbeddingInput::Text(text) = input else {
            return Err(CgError::UnsupportedEvidenceKind {
                kind: input.evidence_kind(),
            });
        };
        if matches!(self.config.source, ModelSource::Disabled) {
            return Err(CgError::TextEmbedderDisabled);
        }
        let loaded = self.load_model()?;
        let vector = self.forward(&loaded, &text)?;
        Ok(EmbeddingOutput {
            embedding_space_id: self.embedding_space().embedding_space_id,
            dimension: self.dimension,
            producer: nomic_producer(),
            vector,
        })
    }

    fn supported_inputs(&self) -> Vec<EvidenceKind> {
        vec![EvidenceKind::Text]
    }

    /// One declaration, on the same evidence as registration: this model answers for the
    /// event-history role only at the canonical dimension, from a live source, over an artifact
    /// this library admits. Installed weights that are not the canonical artifact make the role
    /// unsupported, and the checked entrypoints turn that into a refusal before any hook runs; a
    /// model that is simply not installed yet is instead reported as not available, by name and
    /// path, exactly as plain embedding reports it.
    fn event_search_capability(&self) -> Option<EventSearchCapability> {
        (self.dimension == NOMIC_DEFAULT_DIMENSION
            && !matches!(self.config.source, ModelSource::Disabled)
            && self.artifact_admits_event_search())
        .then(Self::declared_event_search_capability)
    }

    /// Role-aware preparation from this embedder answers under the one canonical event-history
    /// identity, the same identity [`EventHistoryTextEmbedder`] answers under — they are the same
    /// weights, the same tokenizer and the same forward pass, so a passage either one prepares is
    /// embeddable by the other. Plain [`Embedder::embed`] output is unaffected: it stays in the
    /// separate text-default space this embedder declares. A model loaded at any other dimension
    /// is a different vector space and keeps its own dimension-qualified identity instead.
    fn encoding_profile_id(&self) -> String {
        if self.dimension == NOMIC_DEFAULT_DIMENSION {
            BuiltinSpaceRegistry::EVENT_HISTORY_DOCUMENT_QUERY_ID.to_string()
        } else {
            text_space_id_for_dimension(self.dimension)
        }
    }

    fn event_search_slice_token_loss_bound(&self, prefix: &str) -> Option<usize> {
        if !prefix.ends_with(' ') {
            return None;
        }
        // This is the same immutable tokenizer used by preparation, inspected once at load.
        // An unsupported pipeline declines the optimization without changing admission.
        self.load_model_admitted_for_event_search()
            .ok()?
            .slice_token_loss_bound
    }

    fn prepare_document_hook(
        &self,
        text: &str,
        seal: &EventSearchSeal,
    ) -> Result<PreparedEventSearchDocument> {
        self.require_event_search_capability()?;
        let prepared = self.tokenize_event_search_input(EVENT_SEARCH_DOCUMENT_PREFIX, text)?;
        seal.document_builder()
            .token_count(prepared.token_ids.len())
            .byte_boundaries(prepared.byte_boundaries)
            .payload(prepared.token_ids)
            .seal()
    }

    /// Runs only on an inference this model's own document preparation authorized: one carrying
    /// this embedder's reserved encoding profile, this library's own canonical authority for it,
    /// and the document role. Anything else — including an inference an external embedder was
    /// issued for its own profile and relayed here — is refused before the forward pass.
    fn embed_prepared_document_hook(
        &self,
        inference: &EventSearchInference,
    ) -> Result<EmbeddingOutput> {
        self.authorize_inference(inference, EventSearchRole::Document)?;
        self.embed_prepared_token_ids(inference.token_count(), inference.token_ids())
    }

    fn embed_prepared_documents_hook(
        &self,
        inferences: &[EventSearchInference],
    ) -> Result<Vec<EmbeddingOutput>> {
        for inference in inferences {
            self.authorize_inference(inference, EventSearchRole::Document)?;
        }
        self.embed_prepared_token_id_batch(inferences)
    }

    fn prepare_query_hook(
        &self,
        text: &str,
        seal: &EventSearchSeal,
    ) -> Result<PreparedEventSearchQuery> {
        self.require_event_search_capability()?;
        let prepared = self.tokenize_event_search_input(EVENT_SEARCH_QUERY_PREFIX, text)?;
        seal.query_builder()
            .token_count(prepared.token_ids.len())
            .payload(prepared.token_ids)
            .seal()
    }

    /// The query-role counterpart of [`NomicTextEmbedder::embed_prepared_document_hook`], and it
    /// refuses on the same boundary: only an inference authorized under this model's own reserved
    /// encoding profile, for the query role, reaches the forward pass.
    fn embed_prepared_query_hook(
        &self,
        inference: &EventSearchInference,
    ) -> Result<EmbeddingOutput> {
        self.authorize_inference(inference, EventSearchRole::Query)?;
        self.embed_prepared_token_ids(inference.token_count(), inference.token_ids())
    }

    fn embedding_space(&self) -> EmbeddingSpace {
        let embedding_space_id = text_space_id_for_dimension(self.dimension);
        let mut bindings = vec![EmbeddingSpaceBinding {
            embedding_space_id: embedding_space_id.clone(),
            table_name: "evidence".to_string(),
            vector_column: "vector_text".to_string(),
        }];
        if self.dimension == NOMIC_DEFAULT_DIMENSION {
            bindings.push(EmbeddingSpaceBinding {
                embedding_space_id: embedding_space_id.clone(),
                table_name: "digests".to_string(),
                vector_column: "vector_text".to_string(),
            });
        }
        EmbeddingSpace {
            embedding_space_id,
            dimension: self.dimension,
            metric: DistanceMetric::Cosine,
            model_family: "text".to_string(),
            model_name: NOMIC_MODEL_ID.to_string(),
            model_version: "1.5".to_string(),
            supported_evidence_kinds: vec![EvidenceKind::Text],
            default_for_evidence_kind: (self.dimension == NOMIC_DEFAULT_DIMENSION)
                .then_some(EvidenceKind::Text),
            bindings,
        }
    }
}

/// The embedder that answers for the dedicated event-history search space
/// (`event_history_nomic_v1_5_document_query_v1_768`), so that space can be registered into a
/// store alongside the existing Digest/Evidence text-default space.
///
/// It is a thin wrapper, not a second model: it holds the very same loaded
/// [`NomicTextEmbedder`] and hands every preparation and every forward pass straight to it, so
/// registering both spaces costs one set of weights in memory, never two. What the wrapper changes
/// is identity — it declares the event-history space, so a store's registration check
/// (`Store::open` requires an embedder's declared space to equal the space it is registered
/// under) accepts it, and a query in this space is refused comparison with the unlabelled
/// text-default space by the existing cross-space refusal.
///
/// Both roles stay available through the trait: `prepare_document`/`embed_prepared_document` for
/// text being stored for later retrieval, `prepare_query`/`embed_prepared_query` for what a person
/// is searching for. Plain [`Embedder::embed`] names neither role, so it refuses with
/// [`CgError::EventHistoryPlainEmbedRequiresExplicitRole`] rather than guessing: a search phrase
/// silently embedded as a stored passage is exactly the encoding defect this space exists to
/// avoid, and a caller who meant one of the two roles is told to say which.
///
/// The space binds only `event_search_passages.vector_text`, so registering it never contends
/// with the text-default space for `evidence.vector_text` or `digests.vector_text`.
pub struct EventHistoryTextEmbedder {
    model: Arc<NomicTextEmbedder>,
}

impl std::fmt::Debug for EventHistoryTextEmbedder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventHistoryTextEmbedder")
            .field("model", &self.model)
            .finish()
    }
}

impl EventHistoryTextEmbedder {
    /// Answer for the event-history space using a model somebody already holds. The `Arc` handed
    /// in is the one that is stored — share it with the text-default registration and the weights
    /// are loaded once for both, never twice.
    ///
    /// Fallible by contract: this registration advertises the canonical event-history space —
    /// 768 dimensions and the canonical revision of the Nomic text model, read from the installed
    /// cache — so it refuses, with [`CgError::EventHistoryEmbedderConstructionRefused`] naming the
    /// reason, any underlying model that cannot actually serve that. A model loaded at another
    /// dimension is a different vector space; a config that can fetch could reach the network for
    /// weights nobody verified; an explicitly different revision pin names other weights under
    /// the same identity. An absent optional pin is accepted only when the installed `REVISION`
    /// marker proves the canonical revision. Each failure is refused here rather than discovered
    /// later as vectors that do not compare.
    ///
    /// A refusal because nothing is installed is retryable on the same handle: install the model
    /// and call again, and this same model answers for the space. A refusal because the installed
    /// artifact is not the canonical one stands for that installation.
    pub fn over(model: Arc<NomicTextEmbedder>) -> Result<Self> {
        let refuse = |reason: String| CgError::EventHistoryEmbedderConstructionRefused { reason };
        if model.dimension != NOMIC_DEFAULT_DIMENSION {
            return Err(refuse(format!(
                "the event-history space is {NOMIC_DEFAULT_DIMENSION}-dimensional but the underlying model answers at {}",
                model.dimension
            )));
        }
        if matches!(model.config.source, ModelSource::Disabled) {
            return Err(refuse(
                "the underlying model source is disabled, so it can serve no vector at all"
                    .to_string(),
            ));
        }
        if config_can_fetch(&model.config) {
            return Err(refuse(format!(
                "the event-history registration reads only already-installed weights, but the underlying Hugging Face config can fetch because offline={} with network policy {:?}",
                model.config.offline, model.config.network_policy
            )));
        }
        match model.config.pinned_revision.as_deref() {
            Some(revision) if revision == crate::embedder::NOMIC_DEFAULT_REVISION => {}
            None => {}
            Some(revision) => {
                return Err(refuse(format!(
                    "the event-history registration advertises the pinned revision {expected}, but the underlying config pins {revision}",
                    expected = crate::embedder::NOMIC_DEFAULT_REVISION
                )));
            }
        }
        let model_dir = resolve_model_dir(&model.config).map_err(|error| {
            refuse(format!(
                "the pinned event-history artifact could not be resolved locally: {error}"
            ))
        })?;
        match model.event_history_artifact() {
            EventHistoryArtifact::Canonical => {}
            EventHistoryArtifact::Absent => {
                return Err(refuse(format!(
                    "the pinned event-history artifact is not installed at {}; fetch the model before registering the space",
                    model_dir.display()
                )));
            }
            EventHistoryArtifact::Noncanonical => {
                let reason = NomicCacheAccess::acquire(&model_dir)
                    .and_then(|_access| verify_local_event_history_artifact(&model_dir))
                    .err()
                    .map_or_else(
                        || "it is not the canonical installed revision".to_string(),
                        |error| error.to_string(),
                    );
                return Err(refuse(format!(
                    "the pinned event-history artifact at {} could not be verified: {reason}",
                    model_dir.display()
                )));
            }
        }
        Ok(Self { model })
    }

    /// The registration that puts the event-history space into a store, ready to pass to
    /// `Store::open_with_embedder_registrations` beside the text-default one. Fallible for the
    /// same reason as [`EventHistoryTextEmbedder::over`], whose check it reuses.
    pub fn registration(model: Arc<NomicTextEmbedder>) -> Result<EmbedderRegistration> {
        let embedder = Arc::new(Self::over(model)?);
        Ok(EmbedderRegistration {
            space: event_history_embedding_space(),
            embedder,
        })
    }
}

impl Embedder for EventHistoryTextEmbedder {
    /// Refuses rather than guesses. Text reaching this space is either a passage being stored for
    /// later retrieval or a phrase somebody is searching for, and the two are encoded
    /// differently; plain `embed` names neither, so it answers
    /// [`CgError::EventHistoryPlainEmbedRequiresExplicitRole`] and the caller picks
    /// `prepare_document`/`embed_prepared_document` or `prepare_query`/`embed_prepared_query`.
    /// Anything that is not text at all is refused by the kind it actually is.
    fn embed(&self, input: EmbeddingInput) -> Result<EmbeddingOutput> {
        if !matches!(input, EmbeddingInput::Text(_)) {
            return Err(CgError::UnsupportedEvidenceKind {
                kind: input.evidence_kind(),
            });
        }
        Err(CgError::EventHistoryPlainEmbedRequiresExplicitRole)
    }

    fn supported_inputs(&self) -> Vec<EvidenceKind> {
        vec![EvidenceKind::Text]
    }

    fn embedding_space(&self) -> EmbeddingSpace {
        event_history_embedding_space()
    }

    fn event_search_capability(&self) -> Option<EventSearchCapability> {
        self.model.event_search_capability()
    }

    fn event_search_slice_token_loss_bound(&self, prefix: &str) -> Option<usize> {
        self.model.event_search_slice_token_loss_bound(prefix)
    }

    /// The same one canonical event-history identity the underlying model answers under for
    /// role-aware preparation — wrapper and raw model are one encoding, so a value either
    /// prepares is embeddable by the other.
    fn encoding_profile_id(&self) -> String {
        self.model.encoding_profile_id()
    }

    fn prepare_document_hook(
        &self,
        text: &str,
        seal: &EventSearchSeal,
    ) -> Result<PreparedEventSearchDocument> {
        self.model.prepare_document_hook(text, seal)
    }

    fn embed_prepared_document_hook(
        &self,
        inference: &EventSearchInference,
    ) -> Result<EmbeddingOutput> {
        self.model.embed_prepared_document_hook(inference)
    }

    fn embed_prepared_documents_hook(
        &self,
        inferences: &[EventSearchInference],
    ) -> Result<Vec<EmbeddingOutput>> {
        self.model.embed_prepared_documents_hook(inferences)
    }

    fn prepare_query_hook(
        &self,
        text: &str,
        seal: &EventSearchSeal,
    ) -> Result<PreparedEventSearchQuery> {
        self.model.prepare_query_hook(text, seal)
    }

    fn embed_prepared_query_hook(
        &self,
        inference: &EventSearchInference,
    ) -> Result<EmbeddingOutput> {
        self.model.embed_prepared_query_hook(inference)
    }
}

struct LoadedNomicModel {
    model: NomicBertModel,
    tokenizer: Tokenizer,
    // Cached beside its tokenizer, both private and immutable behind the loaded model's Arc.
    // Plain embedding truncates its resulting Encoding, never this preparation tokenizer.
    slice_token_loss_bound: Option<usize>,
    tokenizer_path: String,
    device: Device,
}

/// Certify only the boundary-local Bert/WordPiece pipeline we can prove.
///
/// Bert cleaning, scalar lowercasing/accent removal and Chinese-character separation followed
/// by Bert whitespace/punctuation splitting leave complete interior pre-tokenized units
/// unchanged. Only the two cut units can resegment. A WordPiece unit contributes at most its
/// character limit's tokens, or one UNK; each of these fixed raw added tokens contributes one.
/// None spans whitespace, so a prefix ending in ASCII space cannot join the first source unit.
/// The single-sequence postprocessor preserves A exactly once and adds only fixed CLS/SEP.
/// Thus at most `2 * max(1, limit)` original spans can disappear. No vocabulary serialization
/// or inspection happens per slice, and unsupported configurations still prepare normally.
fn bert_wordpiece_slice_token_loss_bound(tokenizer: &Tokenizer) -> Option<usize> {
    use serde_json::{json, to_value};
    use tokenizers::{Model, models::ModelWrapper};

    if tokenizer.get_truncation().is_some()
        || tokenizer.get_padding().is_some()
        || tokenizer.get_encode_special_tokens()
    {
        return None;
    }
    let ModelWrapper::WordPiece(model) = tokenizer.get_model() else {
        return None;
    };
    if model.unk_token != "[UNK]" || model.continuing_subword_prefix != "##" {
        return None;
    }
    if to_value(tokenizer.get_normalizer()?).ok()?
        != json!({"type": "BertNormalizer", "clean_text": true,
            "handle_chinese_chars": true, "strip_accents": null, "lowercase": true})
        || to_value(tokenizer.get_pre_tokenizer()?).ok()? != json!({"type": "BertPreTokenizer"})
    {
        return None;
    }
    let added = tokenizer.get_added_vocabulary().get_added_tokens_decoder();
    let expected = [
        (0, "[PAD]"),
        (100, "[UNK]"),
        (101, "[CLS]"),
        (102, "[SEP]"),
        (103, "[MASK]"),
    ];
    if added.len() != expected.len() {
        return None;
    }
    for (id, content) in expected {
        let token = added.get(&id)?;
        if token.content != content
            || !token.special
            || token.normalized
            || token.single_word
            || token.lstrip
            || token.rstrip
            || model.token_to_id(content) != Some(id)
        {
            return None;
        }
    }
    let processor = to_value(tokenizer.get_post_processor()?).ok()?;
    if processor["type"] != "TemplateProcessing"
        || processor["single"]
            != json!([
                {"SpecialToken": {"id": "[CLS]", "type_id": 0}},
                {"Sequence": {"id": "A", "type_id": 0}},
                {"SpecialToken": {"id": "[SEP]", "type_id": 0}}
            ])
        || processor["special_tokens"]
            != json!({
                "[CLS]": {"id": "[CLS]", "ids": [101], "tokens": ["[CLS]"]},
                "[SEP]": {"id": "[SEP]", "ids": [102], "tokens": ["[SEP]"]}
            })
    {
        return None;
    }
    model.max_input_chars_per_word.max(1).checked_mul(2)
}

/// Test-only process rendezvous at the actual prepared event-search model invocation. A CLI
/// integration test supplies a directory through `CG_NOMIC_TEST_PREPARED_EMBEDDING_BARRIER`; this
/// writes `reached` after model admission and before the forward pass, then waits for `release`.
/// The feature gate keeps both environment read and synchronization out of ordinary builds.
#[cfg(feature = "test-support")]
fn pause_prepared_embedding_for_test() {
    let Some(directory) = std::env::var_os(NOMIC_PREPARED_EMBEDDING_RENDEZVOUS_ENV) else {
        return;
    };
    let directory = PathBuf::from(directory);
    if file_system::write(directory.join("reached"), b"prepared embedding reached\n").is_err() {
        return;
    }
    while !directory.join("release").exists() {
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(all(test, context_graph_internal_test_support))]
thread_local! {
    static PREPARATION_WORK: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(all(test, context_graph_internal_test_support))]
pub(crate) fn take_preparation_work_for_test() -> (usize, usize) {
    PREPARATION_WORK.with(|work| work.replace((0, 0)))
}

#[cfg(all(test, context_graph_internal_test_support))]
mod bounded_preparation_tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    #[cfg(context_graph_internal_test_support)]
    fn bounded_preparation_nomic_validates_actual_pipeline_and_declines_variations() {
        let tokenizer = Tokenizer::from_file(
            default_cache_root()
                .join(NOMIC_MODEL_ID)
                .join("tokenizer.json"),
        )
        .expect("offline cached tokenizer required");
        assert_eq!(bert_wordpiece_slice_token_loss_bound(&tokenizer), Some(200));
        let original = serde_json::to_value(&tokenizer).unwrap();
        for (path, value) in [
            ("/normalizer/clean_text", json!(false)),
            ("/normalizer/handle_chinese_chars", json!(false)),
            ("/normalizer/strip_accents", json!(false)),
            ("/normalizer/lowercase", json!(false)),
            ("/normalizer", Value::Null),
            ("/pre_tokenizer", json!({"type": "Whitespace"})),
            ("/pre_tokenizer", Value::Null),
            ("/model/type", json!("WordLevel")),
            ("/model/unk_token", json!("other")),
            ("/model/continuing_subword_prefix", json!("other")),
            ("/model/max_input_chars_per_word", json!(usize::MAX)),
            ("/added_tokens/0/content", json!("different")),
            ("/added_tokens/0/single_word", json!(true)),
            ("/added_tokens/0/lstrip", json!(true)),
            ("/added_tokens/0/rstrip", json!(true)),
            ("/added_tokens/0/normalized", json!(true)),
            ("/added_tokens/0/special", json!(false)),
            ("/added_tokens", json!([])),
            ("/post_processor", Value::Null),
            (
                "/post_processor",
                json!({"type":"BertProcessing", "sep":["[SEP]",102], "cls":["[CLS]",101]}),
            ),
            ("/post_processor/single/1/Sequence/id", json!("B")),
            (
                "/post_processor/special_tokens/[CLS]/ids",
                json!([101, 101]),
            ),
            (
                "/post_processor/special_tokens/[CLS]/tokens",
                json!(["different"]),
            ),
        ] {
            let mut variant = original.clone();
            *variant.pointer_mut(path).unwrap() = value;
            let variant = Tokenizer::from_bytes(serde_json::to_vec(&variant).unwrap())
                .unwrap_or_else(|error| panic!("variant must load, {path}: {error}"));
            assert_eq!(
                bert_wordpiece_slice_token_loss_bound(&variant),
                None,
                "{path}"
            );
        }
        let mut variant = tokenizer.clone();
        variant.with_padding(Some(tokenizers::PaddingParams::default()));
        assert_eq!(bert_wordpiece_slice_token_loss_bound(&variant), None);
        let mut variant = tokenizer.clone();
        variant
            .with_truncation(Some(tokenizers::TruncationParams::default()))
            .unwrap();
        assert_eq!(bert_wordpiece_slice_token_loss_bound(&variant), None);
        let mut variant = tokenizer.clone();
        variant.set_encode_special_tokens(true);
        assert_eq!(bert_wordpiece_slice_token_loss_bound(&variant), None);
        let mut variant = tokenizer.clone();
        variant
            .add_special_tokens([tokenizers::AddedToken::from("extra", true)])
            .unwrap();
        assert_eq!(bert_wordpiece_slice_token_loss_bound(&variant), None);
        for (limit, expected) in [(0, 2), (1, 2), (50, 100), (100, 200)] {
            let mut variant = original.clone();
            variant["model"]["max_input_chars_per_word"] = json!(limit);
            let variant = Tokenizer::from_bytes(serde_json::to_vec(&variant).unwrap()).unwrap();
            assert_eq!(
                bert_wordpiece_slice_token_loss_bound(&variant),
                Some(expected)
            );
        }
    }
}

/// Everything that can be established about an installed artifact WITHOUT reading the 547 MB
/// weights file: the revision marker, the weights file's presence and size floor from its
/// directory entry alone, the model config's shape, and that the tokenizer loads.
///
/// This is the admission check — what says an installed directory is the canonical artifact — and
/// it is deliberately separate from [`cache_files_parse`] below, which additionally deserialises
/// the weights. Admission runs on every event-history registration and is cached with the
/// embedder that ran it, so it must never be the thing that reads the weights; the one full read
/// belongs to the loader, which needs the tensors anyway.
fn verify_installed_artifact(
    model_path: &Path,
    tokenizer_path: &Path,
    config_path: &Path,
    revision_path: &Path,
    expected_revision: Option<&str>,
) -> Result<()> {
    check_revision_marker(revision_path, expected_revision)?;
    let model_metadata =
        file_system::metadata(model_path).map_err(|_| CgError::EmbedderModelCorrupt {
            path: model_path.display().to_string(),
        })?;
    if model_metadata.len() < 1_000_000 {
        return Err(CgError::EmbedderModelCorrupt {
            path: model_path.display().to_string(),
        });
    }
    let config_bytes =
        file_system::read(config_path).map_err(|_| CgError::EmbedderModelCorrupt {
            path: config_path.display().to_string(),
        })?;
    let config: NomicBertConfig =
        serde_json::from_slice(&config_bytes).map_err(|_| CgError::EmbedderModelCorrupt {
            path: config_path.display().to_string(),
        })?;
    // Every canonical field the loader would otherwise take on trust. `NomicBertConfig` is
    // `#[serde(default)]`, so a `config.json` of `{}` clears any check that names only the fields
    // an arbitrary checkpoint happens to set.
    if config.n_embd != NOMIC_DEFAULT_DIMENSION
        || config.n_head != 12
        || config.n_layer != 12
        || config.vocab_size != 30_528
        || config.n_inner != 3_072
        || config.type_vocab_size != 2
    {
        return Err(CgError::EmbedderModelCorrupt {
            path: config_path.display().to_string(),
        });
    }
    Tokenizer::from_file(tokenizer_path).map_err(|_| CgError::EmbedderModelCorrupt {
        path: tokenizer_path.display().to_string(),
    })?;
    Ok(())
}

/// The installed-artifact check above, plus a full deserialisation of the weights to confirm the
/// tensors this model needs are present at the shapes it needs. Used on the freshly downloaded
/// artifact, where nothing has read the file yet and a truncated or wrong-model download must be
/// caught before it is cached.
fn cache_files_parse(
    model_path: &Path,
    tokenizer_path: &Path,
    config_path: &Path,
    revision_path: &Path,
    expected_revision: Option<&str>,
) -> Result<()> {
    verify_installed_artifact(
        model_path,
        tokenizer_path,
        config_path,
        revision_path,
        expected_revision,
    )?;
    let model_bytes = file_system::read(model_path).map_err(|_| CgError::EmbedderModelCorrupt {
        path: model_path.display().to_string(),
    })?;
    require_canonical_tensor_set(model_path, &model_bytes)
}

/// Refuses weights that are not the canonical Nomic artifact, by name, shape and element type.
///
/// The vectors this model produces are stored under one reserved space identity, so a
/// different-vocabulary, different-width or half-precision checkpoint that happens to answer at
/// 768 dimensions would be searched as if it were this encoding — quietly wrong neighbours
/// forever, with nothing in the store recording which rows came from which weights. The model's
/// own `config.json` cannot decide this: candle checks every shape against the supplied config, so
/// a checkpoint whose config agrees with itself loads clean. The canonical constant is the only
/// thing that does not move with the artifact.
///
/// Deliberately reads a buffer the caller already has rather than opening the file: the loader
/// reads the weights exactly once and this check rides on that read.
fn require_canonical_tensor_set(model_path: &Path, model_bytes: &[u8]) -> Result<()> {
    let corrupt = || CgError::EmbedderModelCorrupt {
        path: model_path.display().to_string(),
    };
    let tensors = SafeTensors::deserialize(model_bytes).map_err(|_| corrupt())?;
    for (name, shape) in NOMIC_REQUIRED_TENSORS {
        let tensor = tensors.tensor(name).map_err(|_| corrupt())?;
        if tensor.dtype() != SafeTensorDType::F32 || tensor.shape() != *shape {
            return Err(corrupt());
        }
    }
    Ok(())
}

/// Whether a load may install a missing artifact from the configured source, or must answer only
/// for what is already on disk.
///
/// Plain embedding installs on demand when the configuration allows it. The event-search role
/// methods never do: preparing a value, or embedding one the caller already holds, is not a
/// request to download 547 MB, so those paths take [`FetchPolicy::NeverInstall`] and a missing
/// model comes back as [`CgError::EmbedderModelNotAvailable`] naming the resolved directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchPolicy {
    /// Install the artifact if it is missing and the configuration permits a fetch.
    MayInstall,
    /// Never install; report a missing artifact instead.
    NeverInstall,
}

/// What is installed at an embedder's resolved model directory, judged against the canonical
/// event-history encoding. The three cases answer differently on purpose: an ABSENT artifact is a
/// model nobody has installed yet, which the loader reports by name and path, while a
/// NONCANONICAL one is a model that IS installed and is simply not this encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventHistoryArtifact {
    /// The verified canonical artifact.
    Canonical,
    /// Nothing installed at the resolved path — no directory, or missing artifact files.
    Absent,
    /// An installed artifact that is not the canonical one.
    Noncanonical,
}

/// Whether the model files an artifact check would read are installed at all. Presence only: it
/// answers "is there something here to judge", never "is it the right thing", which is
/// [`verify_local_event_history_artifact`]'s job.
fn event_history_artifact_files_installed(model_dir: &Path) -> bool {
    ["model.safetensors", "tokenizer.json", "config.json"]
        .iter()
        .all(|file| cache_file_present(&model_dir.join(file)).unwrap_or(false))
}

/// Whether the directory holds the verified canonical event-history artifact: the installed
/// artifact check above, against the canonical revision the reserved event-history identity names.
/// Never loads the model — see [`verify_installed_artifact`].
fn verify_local_event_history_artifact(model_dir: &Path) -> Result<()> {
    verify_installed_artifact(
        &model_dir.join("model.safetensors"),
        &model_dir.join("tokenizer.json"),
        &model_dir.join("config.json"),
        &model_dir.join("REVISION"),
        Some(crate::embedder::NOMIC_DEFAULT_REVISION),
    )
}

fn check_revision_marker(revision_path: &Path, expected_revision: Option<&str>) -> Result<()> {
    if let Some(expected) = expected_revision {
        let actual = file_system::read_to_string(revision_path).unwrap_or_default();
        if actual.trim() != expected {
            return Err(CgError::EmbedderModelRevisionMismatch {
                expected: expected.to_string(),
                actual: actual.trim().to_string(),
            });
        }
    }
    Ok(())
}

fn load_nomic_artifacts(model_dir: &Path) -> Result<LoadedNomicModel> {
    let model_path = model_dir.join("model.safetensors");
    let tokenizer_path = model_dir.join("tokenizer.json");
    let config_path = model_dir.join("config.json");
    let config_bytes =
        file_system::read(&config_path).map_err(|_| CgError::EmbedderModelCorrupt {
            path: config_path.display().to_string(),
        })?;
    let config: NomicBertConfig =
        serde_json::from_slice(&config_bytes).map_err(|_| CgError::EmbedderModelCorrupt {
            path: config_path.display().to_string(),
        })?;
    let model_bytes =
        file_system::read(&model_path).map_err(|_| CgError::EmbedderModelCorrupt {
            path: model_path.display().to_string(),
        })?;
    // On the read the loader already performs, and before the buffer is handed to candle: an
    // installed directory this process did not download in this run — a user-supplied path, a
    // cache copied between machines, a restored CI cache — reaches here having had its weights
    // checked by nothing else.
    require_canonical_tensor_set(&model_path, &model_bytes)?;
    let device = Device::Cpu;
    let vb =
        VarBuilder::from_buffered_safetensors(model_bytes, DType::F32, &device).map_err(|_| {
            CgError::EmbedderModelCorrupt {
                path: model_path.display().to_string(),
            }
        })?;
    let model = NomicBertModel::load(vb, &config).map_err(|_| CgError::EmbedderModelCorrupt {
        path: model_path.display().to_string(),
    })?;
    let tokenizer =
        Tokenizer::from_file(&tokenizer_path).map_err(|_| CgError::EmbedderModelCorrupt {
            path: tokenizer_path.display().to_string(),
        })?;
    Ok(LoadedNomicModel {
        model,
        slice_token_loss_bound: bert_wordpiece_slice_token_loss_bound(&tokenizer),
        tokenizer,
        tokenizer_path: tokenizer_path.display().to_string(),
        device,
    })
}

#[path = "nomic_publication.rs"]
mod publication;
#[cfg(test)]
use publication::NOMIC_ARTIFACT_FILES;
use publication::{NomicCacheAccess, NomicTemporaryArtifact, cache_io_error, publish_artifact};

/// Each fetch owns its directory, including cleanup after an error or unwinding.
struct NomicDownloadDirectory(PathBuf);

impl NomicDownloadDirectory {
    fn new(model_dir: &Path) -> Result<Self> {
        let path = model_dir.join(format!(".download-{}", uuid::Uuid::new_v4()));
        file_system::create_dir(&path).map_err(|err| {
            CgError::Engine(format!(
                "nomic download directory create failed at {}: {err}",
                path.display()
            ))
        })?;
        Ok(Self(path))
    }
}

impl Drop for NomicDownloadDirectory {
    fn drop(&mut self) {
        let _ = file_system::remove_dir_all(&self.0);
    }
}

// Temporary paths are private installation details; typed failures still name the selected cache.
fn artifact_error_at_destination(error: CgError, staging: &Path, model_dir: &Path) -> CgError {
    let resolved = |path: String| {
        Path::new(&path)
            .strip_prefix(staging)
            .map(|relative| model_dir.join(relative).display().to_string())
            .unwrap_or(path)
    };
    match error {
        CgError::EmbedderModelNotAvailable { source, path } => CgError::EmbedderModelNotAvailable {
            source,
            path: resolved(path),
        },
        CgError::EmbedderModelCorrupt { path } => CgError::EmbedderModelCorrupt {
            path: resolved(path),
        },
        other => other,
    }
}

fn installed_revision(model_dir: &Path) -> Result<Option<String>> {
    let path = model_dir.join("REVISION");
    match file_system::read_to_string(&path) {
        Ok(revision) => Ok(Some(revision.trim().to_string())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(cache_io_error(&path, error)),
    }
}

fn publish_download(
    access: &NomicCacheAccess,
    staging: &Path,
    model_dir: &Path,
    revision: Option<&str>,
) -> Result<()> {
    debug_assert_eq!(access.model_dir, model_dir);
    access.publish(
        staging,
        |staging| {
            cache_files_parse(
                &staging.join("model.safetensors"),
                &staging.join("tokenizer.json"),
                &staging.join("config.json"),
                &staging.join("REVISION"),
                revision,
            )
        },
        |_| Ok(()),
    )
}

fn install_artifact(reader: &mut impl io::Read, destination: &Path, source: &str) -> Result<()> {
    let path = destination.with_file_name(format!(".artifact-{}", uuid::Uuid::new_v4()));
    let out = file_system::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|err| {
            CgError::Engine(format!(
                "nomic artifact temp create failed at {}: {err}",
                path.display()
            ))
        })?;
    // Own the cleanup only after exclusive creation succeeds.
    let temporary = NomicTemporaryArtifact(path);
    // Declare the handle after the guard so unwinding closes it before cleanup too.
    let mut out = out;
    let copied = io::copy(reader, &mut out);
    // Close before cleanup or rename, including on Windows and on a failed read.
    drop(out);
    copied.map_err(|_| CgError::EmbedderModelNotAvailable {
        source: source.to_string(),
        path: destination.display().to_string(),
    })?;
    publish_artifact(&temporary.0, destination)
}

fn download_hf_file(
    repo: &str,
    revision: &str,
    file_name: &str,
    destination: &Path,
    byte_limit: u64,
) -> Result<()> {
    let url = format!("https://huggingface.co/{repo}/resolve/{revision}/{file_name}");
    let mut response = ureq::get(&url)
        .call()
        .map_err(|_| CgError::EmbedderModelNotAvailable {
            source: format!("HuggingFace({repo})"),
            path: destination.display().to_string(),
        })?;
    let mut reader = response.body_mut().with_config().limit(byte_limit).reader();
    install_artifact(&mut reader, destination, &format!("HuggingFace({repo})"))
}

#[cfg(test)]
#[path = "../../tests/support/nomic_artifact_install.rs"]
mod artifact_install_tests;

fn cache_file_present(path: &Path) -> Result<bool> {
    path.try_exists().map_err(|err| {
        CgError::Engine(format!(
            "nomic cache path status failed at {}: {err}",
            path.display()
        ))
    })
}

fn text_space_id_for_dimension(dimension: usize) -> String {
    if dimension == NOMIC_DEFAULT_DIMENSION {
        NOMIC_TEXT_DEFAULT_ID.to_string()
    } else {
        format!("text_default_nomic_v1_5_{dimension}")
    }
}

fn l2_normalize(vector: &mut [f32]) {
    let norm = vector
        .iter()
        .map(|component| component.powi(2))
        .sum::<f32>()
        .sqrt();
    if norm == 0.0 {
        return;
    }
    for component in vector {
        *component /= norm;
    }
}

fn resolve_model_dir(config: &EmbedderConfig) -> Result<PathBuf> {
    match &config.source {
        ModelSource::LocalPath(path) => Ok(path.clone()),
        ModelSource::HuggingFace { .. } => Ok(config
            .cache_dir
            .clone()
            .unwrap_or_else(default_cache_root)
            .join(NOMIC_MODEL_ID)),
        ModelSource::Disabled => Ok(PathBuf::new()),
    }
}

fn source_name(config: &EmbedderConfig) -> String {
    match &config.source {
        ModelSource::LocalPath(path) => format!("LocalPath({})", path.display()),
        ModelSource::HuggingFace { repo } => format!("HuggingFace({repo})"),
        ModelSource::Disabled => "Disabled".to_string(),
    }
}

/// Whether this exact configuration can enter the loader's network fetch branch. A local path is
/// already a complete source and can never fetch; only an online Hugging Face source can.
fn config_can_fetch(config: &EmbedderConfig) -> bool {
    matches!(config.source, ModelSource::HuggingFace { .. })
        && !config.offline
        && !config.network_policy.contains("offline")
}

/// The one directory `nomic-embed-text-v1.5` is cached under, resolved by one rule:
/// `CG_MODEL_CACHE_ROOT` when it is set, else `$XDG_CACHE_HOME/cg/models` when that is set, else
/// the platform cache directory — `%LOCALAPPDATA%\cg\models` on Windows, `$HOME/.cache/cg/models`
/// elsewhere.
///
/// This function is the whole rule, and every layer resolves through it: `cg model fetch` seeds
/// exactly this directory, the offline loader below reads exactly this directory, and
/// `cg_catalog::default_cache_root()` keeps its public name and delegates here. That is what makes
/// "fetch once, then load offline" hold without a second download or a hidden path override.
///
/// Only this `cg/models` cache root is consulted; `cg/embedders` is outside the lookup.
/// `cg model fetch --defaults` populates the root for offline loading.
pub fn default_cache_root() -> PathBuf {
    if let Some(path) = std::env::var_os(CG_MODEL_CACHE_ROOT_ENV) {
        return PathBuf::from(path);
    }
    let base = if cfg!(target_os = "windows") {
        std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
    } else {
        std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .map(|home| home.join(".cache"))
            })
            .unwrap_or_else(std::env::temp_dir)
    };
    base.join("cg").join("models")
}

/// Run `work` and report how long it took. This is the module's ONE raw-clock read, and it is
/// deliberately the only one: an elapsed duration on a fetch or load receipt is an operational
/// measurement, never a persisted timestamp — anything that is stored goes through the substrate's
/// seamed `contextdb_core::Wallclock::now()`.
fn timed<T>(work: impl FnOnce() -> T) -> (T, std::time::Duration) {
    let started = Instant::now();
    let value = work();
    (value, started.elapsed())
}

fn candle_err(err: candle_core::Error) -> CgError {
    CgError::Engine(format!("nomic candle tensor: {err}"))
}

#[cfg(test)]
#[path = "../../tests/support/nomic_cached_admission.rs"]
mod cached_admission_tests;
