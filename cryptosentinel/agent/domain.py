"""
Domain abstraction layer for the causal discovery agent.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime

from .models import Variable, DataSource, VariableType, DataSourceType


class DataSourceBase(ABC):
    """Abstract base class for all data sources."""
    
    def __init__(self, name: str, source_type: DataSourceType):
        self.name = name
        self.source_type = source_type
        self.variables: List[Variable] = []
        self.schema: Optional[Dict[str, Any]] = None
    
    @abstractmethod
    def fetch_data(self, variables: List[str], start_time: Optional[datetime] = None, 
                   end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Fetch data for specified variables."""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """Validate that the data source is accessible."""
        pass
    
    @abstractmethod
    def get_available_variables(self) -> List[Variable]:
        """Get list of available variables from this source."""
        pass


class VariableRegistry:
    """Registry to track discovered variables."""
    
    def __init__(self):
        self._variables: Dict[str, Variable] = {}
        self._by_domain: Dict[str, List[Variable]] = {}
    
    def register(self, variable: Variable):
        """Register a variable."""
        key = self._make_key(variable)
        self._variables[key] = variable
        
        if variable.domain:
            if variable.domain not in self._by_domain:
                self._by_domain[variable.domain] = []
            if variable not in self._by_domain[variable.domain]:
                self._by_domain[variable.domain].append(variable)
    
    def get(self, name: str, domain: Optional[str] = None) -> Optional[Variable]:
        """Get a variable by name and optional domain."""
        key = self._make_key(Variable(name=name, description="", variable_type=VariableType.CONTINUOUS, domain=domain))
        return self._variables.get(key)
    
    def get_by_domain(self, domain: str) -> List[Variable]:
        """Get all variables for a domain."""
        return self._by_domain.get(domain, [])
    
    def list_all(self) -> List[Variable]:
        """List all registered variables."""
        return list(self._variables.values())
    
    def _make_key(self, variable: Variable) -> str:
        """Create a unique key for a variable."""
        return f"{variable.domain or 'default'}:{variable.name}"


@dataclass
class DomainRegistry:
    """Registry for domain-specific knowledge and variables."""
    
    domain_name: str
    description: str
    variables: List[Variable] = field(default_factory=list)
    data_sources: List[DataSource] = field(default_factory=list)
    causal_knowledge: Dict[str, Any] = field(default_factory=dict)
    
    def add_variable(self, variable: Variable):
        """Add a variable to the domain."""
        if variable not in self.variables:
            self.variables.append(variable)
    
    def add_data_source(self, source: DataSource):
        """Add a data source to the domain."""
        if source not in self.data_sources:
            self.data_sources.append(source)
    
    def get_variable(self, name: str) -> Optional[Variable]:
        """Get a variable by name."""
        for var in self.variables:
            if var.name == name:
                return var
        return None
    
    def get_data_source(self, name: str) -> Optional[DataSource]:
        """Get a data source by name."""
        for source in self.data_sources:
            if source.name == name:
                return source
        return None


class SchemaTemplate:
    """Template for generating Avro schemas for different data types."""
    
    @staticmethod
    def for_variable(variable: Variable) -> Dict[str, Any]:
        """Generate Avro schema for a variable."""
        base_schema = {
            "type": "record",
            "name": variable.name.replace(" ", "_").replace("-", "_"),
            "namespace": f"com.cryptosentinel.{variable.domain or 'default'}",
            "fields": [
                {
                    "name": "value",
                    "type": SchemaTemplate._get_avro_type(variable.variable_type),
                },
                {
                    "name": "timestamp",
                    "type": "long",
                    "logicalType": "timestamp-millis",
                },
            ],
        }
        
        # Add domain-specific fields
        if variable.unit:
            base_schema["fields"].append({
                "name": "unit",
                "type": "string",
            })
        
        return base_schema
    
    @staticmethod
    def _get_avro_type(var_type: VariableType) -> str:
        """Map variable type to Avro type."""
        mapping = {
            VariableType.CONTINUOUS: "double",
            VariableType.DISCRETE: "long",
            VariableType.BINARY: "boolean",
            VariableType.CATEGORICAL: "string",
            VariableType.TIME_SERIES: "double",
        }
        return mapping.get(var_type, "string")
    
    @staticmethod
    def for_data_source(source: DataSource) -> Dict[str, Any]:
        """Generate Avro schema for a data source."""
        fields = [
            {
                "name": "timestamp",
                "type": "long",
                "logicalType": "timestamp-millis",
            },
        ]
        
        for var in source.variables:
            fields.append({
                "name": var.name.replace(" ", "_").replace("-", "_"),
                "type": SchemaTemplate._get_avro_type(var.variable_type),
            })
        
        return {
            "type": "record",
            "name": source.name.replace(" ", "_").replace("-", "_"),
            "namespace": "com.cryptosentinel",
            "fields": fields,
        }

