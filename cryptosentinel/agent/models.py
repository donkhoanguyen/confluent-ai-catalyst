"""
Data models for the autonomous causal discovery agent.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class VariableType(str, Enum):
    """Type of variable."""
    CONTINUOUS = "CONTINUOUS"
    DISCRETE = "DISCRETE"
    BINARY = "BINARY"
    CATEGORICAL = "CATEGORICAL"
    TIME_SERIES = "TIME_SERIES"


class DataSourceType(str, Enum):
    """Type of data source."""
    API_REST = "API_REST"
    API_WEBSOCKET = "API_WEBSOCKET"
    KAFKA_TOPIC = "KAFKA_TOPIC"
    DATABASE = "DATABASE"
    FILE = "FILE"


class CausalMethod(str, Enum):
    """Causal inference methods."""
    GRANGER = "GRANGER"
    TRANSFER_ENTROPY = "TRANSFER_ENTROPY"
    PROPENSITY_SCORE_MATCHING = "PROPENSITY_SCORE_MATCHING"
    INSTRUMENTAL_VARIABLES = "INSTRUMENTAL_VARIABLES"
    PC_ALGORITHM = "PC_ALGORITHM"
    DIFFERENCE_IN_DIFFERENCES = "DIFFERENCE_IN_DIFFERENCES"


@dataclass
class Variable:
    """Represents a discovered variable."""
    name: str
    description: str
    variable_type: VariableType
    unit: Optional[str] = None
    domain: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash((self.name, self.domain))
    
    def __eq__(self, other):
        if not isinstance(other, Variable):
            return False
        return self.name == other.name and self.domain == other.domain


@dataclass
class DataSource:
    """Represents a data source for variables."""
    name: str
    source_type: DataSourceType
    endpoint: Optional[str] = None
    api_key: Optional[str] = None
    rate_limit: Optional[int] = None
    variables: List[Variable] = field(default_factory=list)
    schema: Optional[Dict[str, Any]] = None
    kafka_topic: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash((self.name, self.source_type))
    
    def __eq__(self, other):
        if not isinstance(other, DataSource):
            return False
        return self.name == other.name and self.source_type == other.source_type


@dataclass
class Hypothesis:
    """Represents a causal hypothesis."""
    cause: Variable
    effect: Variable
    mechanism: str
    confidence: float  # Initial confidence from Gemini
    suggested_methods: List[CausalMethod]
    potential_confounders: List[Variable] = field(default_factory=list)
    required_data_sources: List[DataSource] = field(default_factory=list)
    hypothesis_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __hash__(self):
        return hash((self.cause, self.effect))
    
    def __eq__(self, other):
        if not isinstance(other, Hypothesis):
            return False
        return self.cause == other.cause and self.effect == other.effect


@dataclass
class CausalResult:
    """Result of a causal inference test."""
    hypothesis: Hypothesis
    method: CausalMethod
    is_significant: bool
    confidence: float
    p_value: Optional[float] = None
    effect_size: Optional[float] = None
    direction: Optional[str] = None
    lead_lag: Optional[int] = None
    sample_size: Optional[int] = None
    confounders_tested: List[Variable] = field(default_factory=list)
    confounders_significant: List[Variable] = field(default_factory=list)
    result_data: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "hypothesis_id": self.hypothesis.hypothesis_id,
            "cause": self.hypothesis.cause.name,
            "effect": self.hypothesis.effect.name,
            "method": self.method.value,
            "is_significant": self.is_significant,
            "confidence": self.confidence,
            "p_value": self.p_value,
            "effect_size": self.effect_size,
            "direction": self.direction,
            "lead_lag": self.lead_lag,
            "sample_size": self.sample_size,
            "confounders_tested": [v.name for v in self.confounders_tested],
            "confounders_significant": [v.name for v in self.confounders_significant],
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class DiscoveryState:
    """State of the causal discovery process."""
    domain: str
    query: str
    hypotheses: List[Hypothesis] = field(default_factory=list)
    results: List[CausalResult] = field(default_factory=list)
    active_data_sources: List[DataSource] = field(default_factory=list)
    discovered_variables: List[Variable] = field(default_factory=list)
    iteration: int = 0
    status: str = "INITIALIZING"  # INITIALIZING, HYPOTHESIS_GENERATION, DATA_COLLECTION, TESTING, REFINING, COMPLETE
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def add_hypothesis(self, hypothesis: Hypothesis):
        """Add a hypothesis if not already present."""
        if hypothesis not in self.hypotheses:
            self.hypotheses.append(hypothesis)
            self.updated_at = datetime.utcnow()
    
    def add_result(self, result: CausalResult):
        """Add a causal result."""
        self.results.append(result)
        self.updated_at = datetime.utcnow()
    
    def add_variable(self, variable: Variable):
        """Add a variable if not already present."""
        if variable not in self.discovered_variables:
            self.discovered_variables.append(variable)
            self.updated_at = datetime.utcnow()
    
    def add_data_source(self, source: DataSource):
        """Add a data source if not already present."""
        if source not in self.active_data_sources:
            self.active_data_sources.append(source)
            self.updated_at = datetime.utcnow()

