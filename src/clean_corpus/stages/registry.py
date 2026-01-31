"""Stage registry.

Stages are configured by name in `build.yaml`.

v0.4 additions:
- expandable PII via detectors + PIIPolicyGate (drop/redact/allow)
- curriculum eligibility tagging stage
- near-dup MinHash and semantic SimHash plugins
- tokenize stage (via tokenizer adapter)

v0.5 additions:
- Unicode NFC normalization
- High-scale deduplication (duplodocus)
- Automated domain tagging (FastText + datamap-rs)
- Unified configuration with global/entry-level processing

"""

from __future__ import annotations
from typing import Dict, List, Optional, Any
from ..policies.loader import load_yaml
from .impl import LicenseGate, Sanitize, ExactDedup, QualityGate
from .pii_policy import PIIPolicyGate
from .curriculum_eligibility import CurriculumEligibility
from .unicode_normalize import UnicodeNormalize

# Optional imports for stages with heavy dependencies
try:
    from .near_dup_minhash import NearDupMinHash
except ImportError:
    NearDupMinHash = None

try:
    from .semantic_simhash import SemanticSimHash
except ImportError:
    SemanticSimHash = None

try:
    from .tokenize_plugin import TokenizeStage
except ImportError:
    TokenizeStage = None

try:
    from .duplodocus_dedup import DuplodocusDedup
except ImportError:
    DuplodocusDedup = None

try:
    from .domain_tagging import DomainTagging
except ImportError:
    DomainTagging = None

try:
    from .global_dedup import GlobalDedupStage
    from ..fingerprints import GlobalFingerprintManager
    from ..storage.base import get_storage_backend
except ImportError:
    GlobalDedupStage = None
    GlobalFingerprintManager = None
    get_storage_backend = None

def make_stages(
    stage_names: List[str], 
    policies: Dict[str, str], 
    *,
    tokenizer_name: str = "custom_tok",
    global_processing: Optional[Dict[str, Any]] = None,
    source_configs: Optional[List[Dict[str, Any]]] = None
):
    """
    Create processing stages from configuration.
    
    Args:
        stage_names: List of stage names to include
        policies: Policy file paths
        tokenizer_name: Tokenizer name for tokenize stage
        global_processing: Global processing configuration
        source_configs: Source configurations (for entry-level overrides)
    """
    global_processing = global_processing or {}
    source_configs = source_configs or []
    
    lic_pol = load_yaml(policies["licenses"])
    qual_pol = load_yaml(policies["quality"])
    pii_pol = load_yaml(policies["pii"])
    # Curriculum policy may be embedded in build.yaml under policies.curriculum (optional)
    cur_pol = load_yaml(policies["curriculum"]) if "curriculum" in policies else {"windows":[4096,16384,65536,262144]}

    # Get global processing settings
    unicode_norm_enabled = global_processing.get("unicode_normalize", True)
    dedup_config = global_processing.get("deduplication") or {}
    global_fingerprints_config = global_processing.get("global_fingerprints") or {}
    domain_tagging_config = global_processing.get("domain_tagging") or {}

    name_to_stage = {
        "license_gate": LicenseGate(lic_pol),
        "sanitize": Sanitize(),
        "unicode_normalize": UnicodeNormalize(enabled=unicode_norm_enabled),
        "exact_dedup": ExactDedup(),
        "quality_gate": QualityGate(qual_pol),
        "pii_policy_gate": PIIPolicyGate(pii_pol),
        "curriculum_eligibility": CurriculumEligibility(cur_pol),
    }
    
    # Add global fingerprint deduplication if enabled (global, persistent, cross-dataset)
    if global_fingerprints_config.get("enabled", False) and GlobalDedupStage is not None and GlobalFingerprintManager is not None:
        gfc = global_fingerprints_config
        # Read from child entries (simhash: { enabled, max_hamming }) with fallback to flat keys
        def _enabled(key: str, default: bool = True) -> bool:
            v = gfc.get(key)
            if isinstance(v, dict):
                return v.get("enabled", default)
            if v is not None:
                return bool(v)
            return default

        def _val(child_key: str, param: str, default: Any) -> Any:
            child = gfc.get(child_key)
            if isinstance(child, dict) and param in child:
                return child[param]
            # Legacy flat keys: simhash_max_hamming, minhash_threshold, minhash_ngram, minhash_num_perm, chunk_size, chunk_overlap
            flat = param if child_key == "chunk_hash" else f"{child_key}_{param}"
            return gfc.get(flat, default)

        storage = None
        if get_storage_backend and gfc.get("storage"):
            storage = get_storage_backend(gfc["storage"])
        root_path = gfc.get("root_path", "fingerprints_global")
        gf_manager = GlobalFingerprintManager(
            storage=storage,
            root_path=root_path,
            simhash_enabled=_enabled("simhash", True),
            minhash_enabled=_enabled("minhash", True),
            chunk_hash_enabled=_enabled("chunk_hash", True),
            simhash_max_hamming=int(_val("simhash", "max_hamming", 3)),
            minhash_threshold=float(_val("minhash", "threshold", 0.9)),
            minhash_ngram=int(_val("minhash", "ngram", 5)),
            minhash_num_perm=int(_val("minhash", "num_perm", 128)),
            chunk_size=int(_val("chunk_hash", "chunk_size", 512)),
            chunk_overlap=int(_val("chunk_hash", "chunk_overlap", 0)),
            fingerprint_version=gfc.get("fingerprint_version", "v1"),
            source_priority=gfc.get("source_priority"),
            document_type_priority=gfc.get("document_type_priority"),
            source_to_document_type=gfc.get("source_to_document_type") or {},
        )
        name_to_stage["global_dedup"] = GlobalDedupStage(gf_manager)

    # Add duplodocus deduplication if enabled
    if dedup_config.get("enabled", False) and dedup_config.get("method") == "duplodocus":
        duplodocus_cfg = dedup_config.get("duplodocus", {})
        if DuplodocusDedup is not None:
            name_to_stage["duplodocus_dedup"] = DuplodocusDedup(
                exact_match=duplodocus_cfg.get("exact_match", True),
                minhash=duplodocus_cfg.get("minhash", True),
                disk_based=duplodocus_cfg.get("disk_based", True),
                threshold=duplodocus_cfg.get("threshold", 0.9),
            )
    
    # Add domain tagging if enabled
    if domain_tagging_config.get("enabled", False):
        if DomainTagging is not None:
            name_to_stage["domain_tagging"] = DomainTagging(
                fasttext_model_path=domain_tagging_config.get("fasttext_model"),
                datamap_config=domain_tagging_config.get("datamap_config"),
                enabled=True,
            )
    
    # Add optional stages if available
    if NearDupMinHash is not None:
        name_to_stage["near_dup_minhash"] = NearDupMinHash(threshold=0.9, hard_reject=False)
    if SemanticSimHash is not None:
        name_to_stage["semantic_simhash"] = SemanticSimHash()
    if TokenizeStage is not None:
        name_to_stage["tokenize"] = TokenizeStage(tokenizer_name=tokenizer_name)

    stages = []
    for n in stage_names:
        if n not in name_to_stage:
            raise ValueError(f"Unknown stage: {n}. Register it in clean_corpus.stages.registry")
        stages.append(name_to_stage[n])
    return stages
