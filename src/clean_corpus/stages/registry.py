"""Stage registry.

Stages are configured by name in `build.yaml`.

v0.4 additions:
- expandable PII via detectors + PIIPolicyGate (drop/redact/allow)
- curriculum eligibility tagging stage
- near-dup MinHash and semantic SimHash plugins
- tokenize stage (via tokenizer adapter)

"""

from __future__ import annotations
from typing import Dict, List
from ..policies.loader import load_yaml
from .impl import LicenseGate, Sanitize, ExactDedup, QualityGate
from .pii_policy import PIIPolicyGate
from .curriculum_eligibility import CurriculumEligibility

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

def make_stages(stage_names: List[str], policies: Dict[str, str], *, tokenizer_name: str = "custom_tok"):
    lic_pol = load_yaml(policies["licenses"])
    qual_pol = load_yaml(policies["quality"])
    pii_pol = load_yaml(policies["pii"])
    # Curriculum policy may be embedded in build.yaml under policies.curriculum (optional)
    cur_pol = load_yaml(policies["curriculum"]) if "curriculum" in policies else {"windows":[4096,16384,65536,262144]}

    name_to_stage = {
        "license_gate": LicenseGate(lic_pol),
        "sanitize": Sanitize(),
        "exact_dedup": ExactDedup(),
        "quality_gate": QualityGate(qual_pol),
        "pii_policy_gate": PIIPolicyGate(pii_pol),
        "curriculum_eligibility": CurriculumEligibility(cur_pol),
    }
    
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
