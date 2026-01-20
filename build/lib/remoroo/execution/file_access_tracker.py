"""File access tracker for ensuring files are read in full before editing."""

from typing import Set, Dict, Any, Tuple, Optional


class FileAccessTracker:
    """Tracks which files have been read in full vs chunks."""
    
    def __init__(self):
        self.full_files: Set[str] = set()  # Files read in full
        self.chunked_files: Set[str] = set()  # Files read as chunks
    
    def mark_full(self, file_path: str):
        """Mark a file as read in full."""
        self.full_files.add(file_path)
        # Remove from chunked if it was there (upgrade to full)
        if file_path in self.chunked_files:
            self.chunked_files.remove(file_path)
    
    def mark_chunked(self, file_path: str):
        """Mark a file as read in chunks only."""
        # Only add if not already in full_files
        if file_path not in self.full_files:
            self.chunked_files.add(file_path)
    
    def can_edit(self, file_path: str) -> bool:
        """Check if file can be edited (must be read in full)."""
        return file_path in self.full_files
    
    def can_create(self, file_path: str) -> bool:
        """Check if file can be created (always allowed for new files)."""
        return True  # New files can always be created
    
    def get_status(self, file_path: str) -> str:
        """Get access status for a file."""
        if file_path in self.full_files:
            return "full"
        elif file_path in self.chunked_files:
            return "chunked"
        else:
            return "unknown"
    
    def update_from_context_pack(self, context_pack: Dict[str, Any]):
        """Update tracker from context pack."""
        files = context_pack.get("files", [])
        for file_entry in files:
            if not isinstance(file_entry, dict):
                continue
            file_path = file_entry.get("path")
            if not file_path:
                continue
            
            # Check if file is chunked
            if file_entry.get("chunked", False):
                self.mark_chunked(file_path)
            else:
                # File is included in full
                self.mark_full(file_path)
    
    def update_from_hydration_response(self, hydration_response: Dict[str, Any]):
        """
        Update tracker from hydration response (Phase 3: Large-repo migration).
        
        Files provided via hydration are marked as read in full since they're
        provided specifically for editing purposes.
        """
        if not hydration_response:
            return
        
        # Mark files from full-file hydration as read in full
        full_files = hydration_response.get("files", [])
        for file_info in full_files:
            file_path = file_info.get("file_path")
            if file_path:
                self.mark_full(file_path)
        
        # Mark files from snippet hydration as read in full (they're provided for editing)
        snippets = hydration_response.get("snippets", [])
        for snippet in snippets:
            file_path = snippet.get("file_path")
            if file_path:
                self.mark_full(file_path)
    
    def validate_patch_intent(
        self, 
        patch_intent_bundle: Dict[str, Any],
        repo_root: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Validate that all files in patch intent can be edited or created.
        
        Args:
            patch_intent_bundle: Patch intent bundle with target_files
            repo_root: Optional repository root to check if files exist
            
        Returns:
            (is_valid, error_message)
        """
        target_files = patch_intent_bundle.get("target_files", [])
        
        for file_path in target_files:
            # Check if file exists in repository
            file_exists = False
            if repo_root:
                import os
                abs_path = os.path.join(repo_root, file_path)
                file_exists = os.path.exists(abs_path) and os.path.isfile(abs_path)
            
            if file_exists:
                # File exists - must be read in full before editing
                if not self.can_edit(file_path):
                    return False, (
                        f"Cannot edit {file_path}: file not read in full. "
                        f"Add to focus_files or read file completely first. "
                        f"Current status: {self.get_status(file_path)}"
                    )
            else:
                # File doesn't exist - creation is always allowed
                # No validation needed for new files
                pass
        
        return True, ""

