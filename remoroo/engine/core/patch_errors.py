from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class PatchError:
    """Structured patch application error."""
    error_type: str  # "syntax_error", "search_not_found", "ambiguous_match", "file_not_found", "validation_failed"
    file_path: str
    message: str
    edit_kind: Optional[str] = None
    line_number: Optional[int] = None
    search_preview: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PatchError":
        return cls(**data)
