"""KnowledgeGraphDB — lightweight per-repo knowledge graph (Phase 5).

Persisted at `.remoroo/knowledge_graph.db` (local SQLite). Captures:
  - Entities: files, classes, functions with summaries
  - Relationships: imports, calls, inherits, depends_on
  - Decisions: patches, investigations, diagnoses from past runs

Unlike the static `repo_index.json`, this graph grows with every Remoroo run
and captures cross-run decision provenance.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    summary TEXT,
    last_indexed_at TEXT,
    UNIQUE(path)
);

CREATE TABLE IF NOT EXISTS relationships (
    id INTEGER PRIMARY KEY,
    source_id INTEGER REFERENCES entities(id),
    target_id INTEGER REFERENCES entities(id),
    rel_type TEXT NOT NULL,
    metadata TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY,
    run_id TEXT NOT NULL,
    entity_id INTEGER REFERENCES entities(id),
    decision_type TEXT,
    rationale TEXT,
    outcome TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_entity_path ON entities(path);
CREATE INDEX IF NOT EXISTS idx_rel_source ON relationships(source_id);
CREATE INDEX IF NOT EXISTS idx_rel_target ON relationships(target_id);
CREATE INDEX IF NOT EXISTS idx_decisions_entity ON decisions(entity_id);
"""


class KnowledgeGraphDB:
    """SQLite-backed per-repo knowledge graph."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA_SQL)
        self._conn.commit()

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    # ─── Entities ───

    def upsert_entity(
        self,
        path: str,
        entity_type: str,
        summary: str = "",
    ) -> int:
        """Insert or update an entity. Returns the entity id."""
        cursor = self._conn.execute(
            """INSERT INTO entities (path, entity_type, summary, last_indexed_at)
               VALUES (?, ?, ?, datetime('now'))
               ON CONFLICT(path) DO UPDATE SET
                 entity_type = EXCLUDED.entity_type,
                 summary = CASE WHEN EXCLUDED.summary != '' THEN EXCLUDED.summary ELSE entities.summary END,
                 last_indexed_at = datetime('now')""",
            (path, entity_type, summary),
        )
        self._conn.commit()
        row = self._conn.execute("SELECT id FROM entities WHERE path = ?", (path,)).fetchone()
        return row["id"] if row else cursor.lastrowid

    def get_entity(self, path: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute("SELECT * FROM entities WHERE path = ?", (path,)).fetchone()
        return dict(row) if row else None

    def search_entities(self, prefix: str = "", entity_type: str = "", limit: int = 50) -> List[Dict[str, Any]]:
        query = "SELECT * FROM entities WHERE 1=1"
        params: list = []
        if prefix:
            query += " AND path LIKE ?"
            params.append(f"{prefix}%")
        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type)
        query += " ORDER BY last_indexed_at DESC LIMIT ?"
        params.append(limit)
        return [dict(r) for r in self._conn.execute(query, params).fetchall()]

    # ─── Relationships ───

    def add_relationship(
        self,
        source_path: str,
        target_path: str,
        rel_type: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Add a relationship between two entities (auto-creates entities if needed)."""
        source_id = self.upsert_entity(source_path, "unknown")
        target_id = self.upsert_entity(target_path, "unknown")

        existing = self._conn.execute(
            "SELECT id FROM relationships WHERE source_id = ? AND target_id = ? AND rel_type = ?",
            (source_id, target_id, rel_type),
        ).fetchone()
        if existing:
            return

        self._conn.execute(
            "INSERT INTO relationships (source_id, target_id, rel_type, metadata) VALUES (?, ?, ?, ?)",
            (source_id, target_id, rel_type, json.dumps(metadata or {})),
        )
        self._conn.commit()

    def query_dependents(self, entity_path: str, depth: int = 1) -> List[Dict[str, Any]]:
        """Find what depends on this entity (incoming relationships)."""
        entity = self.get_entity(entity_path)
        if not entity:
            return []

        results = []
        visited = set()
        self._traverse_dependents(entity["id"], depth, results, visited)
        return results

    def _traverse_dependents(self, entity_id: int, depth: int, results: List, visited: set):
        if depth <= 0 or entity_id in visited:
            return
        visited.add(entity_id)

        rows = self._conn.execute(
            """SELECT e.path, e.entity_type, r.rel_type
               FROM relationships r
               JOIN entities e ON e.id = r.source_id
               WHERE r.target_id = ?""",
            (entity_id,),
        ).fetchall()

        for row in rows:
            results.append(dict(row))
            source_entity = self._conn.execute("SELECT id FROM entities WHERE path = ?", (row["path"],)).fetchone()
            if source_entity:
                self._traverse_dependents(source_entity["id"], depth - 1, results, visited)

    def query_impact_radius(self, entity_path: str, depth: int = 2) -> Dict[str, Any]:
        """Compute the impact radius of changing an entity."""
        dependents = self.query_dependents(entity_path, depth=depth)
        affected_files = set()
        for dep in dependents:
            path = dep.get("path", "")
            if "::" in path:
                path = path.split("::")[0]
            affected_files.add(path)

        return {
            "entity": entity_path,
            "direct_dependents": len([d for d in dependents if d]),
            "affected_files": sorted(affected_files),
            "depth": depth,
        }

    # ─── Decisions ───

    def record_decision(
        self,
        run_id: str,
        entity_path: str,
        decision_type: str,
        rationale: str = "",
        outcome: str = "",
    ):
        """Record a decision made about an entity during a run."""
        entity_id = self.upsert_entity(entity_path, "unknown")
        self._conn.execute(
            "INSERT INTO decisions (run_id, entity_id, decision_type, rationale, outcome) VALUES (?, ?, ?, ?, ?)",
            (run_id, entity_id, decision_type, rationale, outcome),
        )
        self._conn.commit()

    def query_decision_history(self, entity_path: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get decision history for an entity."""
        entity = self.get_entity(entity_path)
        if not entity:
            return []

        rows = self._conn.execute(
            """SELECT d.run_id, d.decision_type, d.rationale, d.outcome, d.created_at
               FROM decisions d
               WHERE d.entity_id = ?
               ORDER BY d.created_at DESC
               LIMIT ?""",
            (entity["id"], limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ─── Summary ───

    def get_summary(self) -> Dict[str, Any]:
        """Get a high-level summary of the knowledge graph."""
        entity_count = self._conn.execute("SELECT COUNT(*) as c FROM entities").fetchone()["c"]
        rel_count = self._conn.execute("SELECT COUNT(*) as c FROM relationships").fetchone()["c"]
        decision_count = self._conn.execute("SELECT COUNT(*) as c FROM decisions").fetchone()["c"]

        entity_types = {}
        for row in self._conn.execute("SELECT entity_type, COUNT(*) as c FROM entities GROUP BY entity_type").fetchall():
            entity_types[row["entity_type"]] = row["c"]

        rel_types = {}
        for row in self._conn.execute("SELECT rel_type, COUNT(*) as c FROM relationships GROUP BY rel_type").fetchall():
            rel_types[row["rel_type"]] = row["c"]

        return {
            "total_entities": entity_count,
            "total_relationships": rel_count,
            "total_decisions": decision_count,
            "entity_types": entity_types,
            "relationship_types": rel_types,
        }

    # ─── Bulk Population ───

    def populate_from_index(self, repo_index: Dict[str, Any]):
        """Bulk-populate entities and relationships from a repo_index.json."""
        files = repo_index.get("files", {})
        for file_path, file_info in files.items():
            self.upsert_entity(
                path=file_path,
                entity_type="file",
                summary=f"{file_info.get('language', '?')} file, {file_info.get('size_bytes', 0)} bytes",
            )

            for imp in file_info.get("imports", []):
                if isinstance(imp, str) and imp:
                    self.add_relationship(file_path, imp, "imports")

            for sym_id in file_info.get("top_level_symbols", []):
                if isinstance(sym_id, str) and sym_id:
                    parts = sym_id.split(":")
                    name = parts[-1].split("#")[0] if parts else sym_id
                    self.upsert_entity(
                        path=f"{file_path}::{name}",
                        entity_type="symbol",
                    )
                    self.add_relationship(f"{file_path}::{name}", file_path, "defined_in")

        symbols = repo_index.get("symbols", {})
        for sym_id, sym_info in symbols.items():
            self.upsert_entity(
                path=sym_info.get("qualified_name", sym_id),
                entity_type=sym_info.get("kind", "symbol"),
                summary=sym_info.get("signature", "")[:200],
            )
