"""
Discovery configuration loader.

Reads sources.yaml and resolves all paths against CIVICTWIN_ROOT.
Set the CIVICTWIN_ROOT environment variable to override the path in sources.yaml.

Usage:
    from discovery.config import get_config
    cfg = get_config()

    if cfg.enabled("document_center"):
        out = cfg.output_dir("document_center")

    for f in cfg.collection_files("assessor"):
        print(f["id"], f["abs_path"])
"""

import os
from pathlib import Path

import yaml

_SOURCES_FILE = Path(__file__).parent / "sources.yaml"

# Local directory for logs, reports, and intermediate files (not on the volume).
# Gitignored — never committed.
LOCAL_OUTPUT_DIR = Path(__file__).parent / "output"


class SourceConfig:
    def __init__(self, path: Path = _SOURCES_FILE):
        with open(path) as f:
            raw = yaml.safe_load(f)

        env_root = os.environ.get("CIVICTWIN_ROOT")
        self.root = Path(env_root) if env_root else Path(raw["civictwin_root"])
        self._sources: dict = raw["sources"]

        db_env = os.environ.get("DB_DIR")
        self._db_dir = Path(db_env) if db_env else self.root / "db"

    def enabled(self, source_id: str) -> bool:
        return self._sources.get(source_id, {}).get("enabled", False)

    def source(self, source_id: str) -> dict:
        """Raw config dict for a source."""
        return self._sources[source_id]

    def output_dir(self, source_id: str) -> Path:
        """Resolved output directory for a scrape source."""
        return self.root / self._sources[source_id]["output_dir"]

    def collection_files(self, source_id: str) -> list[dict]:
        """
        Collection source files with abs_path resolved against civictwin_root.
        Returns a list of dicts — all original fields plus 'abs_path'.
        """
        result = []
        for entry in self._sources[source_id]["files"]:
            resolved = dict(entry)
            resolved["abs_path"] = self.root / entry["path"]
            result.append(resolved)
        return result

    @property
    def db_dir(self) -> Path:
        return self._db_dir

    def db_path(self, name: str) -> Path:
        """Return path to a named database file under db_dir."""
        return self._db_dir / f"{name}.db"

    def registry_override_robots(self) -> bool:
        return self._sources["registry"].get("override_robots", False)


_instance: SourceConfig | None = None


def get_config() -> SourceConfig:
    global _instance
    if _instance is None:
        _instance = SourceConfig()
    return _instance
