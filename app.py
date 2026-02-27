import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import uuid
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "storage"
DB_PATH = BASE_DIR / "brain.db"

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg"}
AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".ogg", ".flac", ".aac"}
IGNORE_EXTS = {".json", ".html", ".md"}

CATEGORY_RULES = {
    "housing court case": ["housing court", "eviction", "lease", "landlord", "tenant"],
    "dog": ["dog", "puppy", "canine", "vet"],
    "restaurant": ["restaurant", "menu", "reservation", "chef", "dining"],
}

app = FastAPI(title="Brain")
app.mount("/storage", StaticFiles(directory=str(STORAGE_DIR)), name="storage")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-") or "category"


def init_db() -> None:
    STORAGE_DIR.mkdir(exist_ok=True)
    conn = get_conn()
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS exports (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            status TEXT NOT NULL,
            source_zip_path TEXT NOT NULL,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS conversations (
            id TEXT PRIMARY KEY,
            export_id TEXT NOT NULL,
            external_id TEXT,
            title TEXT,
            conversation_date TEXT,
            raw_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(export_id) REFERENCES exports(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            export_id TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            role TEXT,
            content_text TEXT,
            created_at TEXT,
            FOREIGN KEY(export_id) REFERENCES exports(id) ON DELETE CASCADE,
            FOREIGN KEY(conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS assets (
            id TEXT PRIMARY KEY,
            export_id TEXT NOT NULL,
            conversation_id TEXT,
            message_id TEXT,
            asset_type TEXT NOT NULL,
            original_name TEXT NOT NULL,
            storage_path TEXT NOT NULL,
            byte_size INTEGER NOT NULL,
            checksum_sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(export_id) REFERENCES exports(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS categories (
            id TEXT PRIMARY KEY,
            export_id TEXT NOT NULL,
            name TEXT NOT NULL,
            slug TEXT NOT NULL,
            is_system INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            UNIQUE(export_id, slug),
            FOREIGN KEY(export_id) REFERENCES exports(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS conversation_categories (
            conversation_id TEXT NOT NULL,
            category_id TEXT NOT NULL,
            source TEXT NOT NULL,
            confidence REAL,
            created_at TEXT NOT NULL,
            PRIMARY KEY(conversation_id, category_id),
            FOREIGN KEY(conversation_id) REFERENCES conversations(id) ON DELETE CASCADE,
            FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()
    conn.close()


def classify_asset(file_name: str) -> Optional[str]:
    ext = Path(file_name).suffix.lower()
    if ext in IMAGE_EXTS:
        return "image"
    if ext in AUDIO_EXTS:
        return "audio"
    if ext in IGNORE_EXTS:
        return None
    return "file"


def ensure_category(conn: sqlite3.Connection, export_id: str, name: str, is_system: int = 1) -> str:
    slug = slugify(name)
    existing = conn.execute(
        "SELECT id FROM categories WHERE export_id = ? AND slug = ?", (export_id, slug)
    ).fetchone()
    if existing:
        return existing["id"]
    category_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO categories (id, export_id, name, slug, is_system, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (category_id, export_id, name, slug, is_system, datetime.utcnow().isoformat()),
    )
    return category_id


def safe_extract(zip_path: Path, extract_to: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            member_path = Path(member.filename)
            if member_path.is_absolute() or ".." in member_path.parts:
                continue
            target = extract_to / member.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                with zf.open(member, "r") as source, open(target, "wb") as dest:
                    shutil.copyfileobj(source, dest)


def parse_iso_or_none(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    try:
        return datetime.utcfromtimestamp(float(ts)).isoformat()
    except Exception:
        return None


def parse_export(export_id: str, zip_path: Path) -> None:
    conn = get_conn()
    conn.execute("UPDATE exports SET status = ? WHERE id = ?", ("processing", export_id))
    conn.commit()

    export_dir = STORAGE_DIR / "exports" / export_id
    extracted_dir = export_dir / "extracted"
    assets_dir = export_dir / "assets"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    assets_dir.mkdir(parents=True, exist_ok=True)

    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            safe_extract(zip_path, tmp_path)

            conv_file = next(tmp_path.rglob("conversations.json"), None)
            if not conv_file:
                raise ValueError("Missing conversations.json in ZIP export")

            with open(conv_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if not isinstance(payload, list):
                raise ValueError("conversations.json must contain a list")

            # Copy extracted files and register assets.
            file_map = {}
            for file in tmp_path.rglob("*"):
                if not file.is_file() or file == conv_file:
                    continue
                rel = file.relative_to(tmp_path)
                target = extracted_dir / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file, target)

                asset_type = classify_asset(file.name)
                if not asset_type:
                    continue
                checksum = hashlib.sha256(file.read_bytes()).hexdigest()
                asset_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO assets (id, export_id, conversation_id, message_id, asset_type, original_name, storage_path, byte_size, checksum_sha256, created_at)
                    VALUES (?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        asset_id,
                        export_id,
                        asset_type,
                        file.name,
                        str(target.relative_to(STORAGE_DIR)),
                        file.stat().st_size,
                        checksum,
                        datetime.utcnow().isoformat(),
                    ),
                )
                file_map.setdefault(file.name.lower(), []).append(asset_id)

            for conv in payload:
                conv_id = str(uuid.uuid4())
                external_id = conv.get("id")
                title = conv.get("title") or "Untitled"
                create_time = conv.get("create_time")
                conversation_date = parse_iso_or_none(create_time)

                conn.execute(
                    """
                    INSERT INTO conversations (id, export_id, external_id, title, conversation_date, raw_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        conv_id,
                        export_id,
                        external_id,
                        title,
                        conversation_date,
                        json.dumps(conv),
                        datetime.utcnow().isoformat(),
                    ),
                )

                mapping = conv.get("mapping") or {}
                nodes = []
                for node in mapping.values():
                    message = (node or {}).get("message")
                    if message:
                        nodes.append(message)

                def sort_key(msg):
                    ct = msg.get("create_time")
                    try:
                        return float(ct) if ct is not None else float("inf")
                    except Exception:
                        return float("inf")

                full_text_parts = [title]
                for msg in sorted(nodes, key=sort_key):
                    content = msg.get("content") or {}
                    parts = content.get("parts") or []
                    text = "\n".join(str(p) for p in parts if p is not None).strip()
                    role = ((msg.get("author") or {}).get("role")) or "unknown"
                    msg_id = str(uuid.uuid4())
                    msg_time = parse_iso_or_none(msg.get("create_time"))

                    conn.execute(
                        """
                        INSERT INTO messages (id, export_id, conversation_id, role, content_text, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (msg_id, export_id, conv_id, role, text, msg_time),
                    )

                    if text:
                        full_text_parts.append(text)
                        lowered = text.lower()
                        for file_name, asset_ids in file_map.items():
                            if file_name in lowered:
                                for asset_id in asset_ids:
                                    conn.execute(
                                        "UPDATE assets SET conversation_id = COALESCE(conversation_id, ?), message_id = COALESCE(message_id, ?) WHERE id = ?",
                                        (conv_id, msg_id, asset_id),
                                    )

                conversation_text = "\n".join(full_text_parts).lower()
                matched_any = False
                for category_name, keywords in CATEGORY_RULES.items():
                    if any(kw in conversation_text for kw in keywords):
                        matched_any = True
                        cat_id = ensure_category(conn, export_id, category_name, is_system=1)
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO conversation_categories (conversation_id, category_id, source, confidence, created_at)
                            VALUES (?, ?, 'auto', 1.0, ?)
                            """,
                            (conv_id, cat_id, datetime.utcnow().isoformat()),
                        )

                if not matched_any:
                    unc_id = ensure_category(conn, export_id, "uncategorized", is_system=1)
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO conversation_categories (conversation_id, category_id, source, confidence, created_at)
                        VALUES (?, ?, 'auto', 1.0, ?)
                        """,
                        (conv_id, unc_id, datetime.utcnow().isoformat()),
                    )

            conn.execute(
                "UPDATE exports SET status = ?, error_message = NULL WHERE id = ?",
                ("ready", export_id),
            )
            conn.commit()
    except Exception as exc:
        conn.execute(
            "UPDATE exports SET status = ?, error_message = ? WHERE id = ?",
            ("failed", str(exc), export_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()


@app.on_event("startup")
def startup_event() -> None:
    init_db()


@app.get("/")
def home(request: Request):
    conn = get_conn()
    exports = conn.execute(
        "SELECT * FROM exports ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return templates.TemplateResponse("index.html", {"request": request, "exports": exports})


@app.post("/uploads")
async def upload_export(name: str = Form(...), file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Please upload a .zip file")

    export_id = str(uuid.uuid4())
    export_dir = STORAGE_DIR / "exports" / export_id / "source"
    export_dir.mkdir(parents=True, exist_ok=True)
    zip_path = export_dir / "export.zip"

    with open(zip_path, "wb") as out:
        out.write(await file.read())

    conn = get_conn()
    conn.execute(
        "INSERT INTO exports (id, name, created_at, status, source_zip_path) VALUES (?, ?, ?, ?, ?)",
        (export_id, name, datetime.utcnow().isoformat(), "uploaded", str(zip_path.relative_to(STORAGE_DIR))),
    )
    conn.commit()
    conn.close()

    parse_export(export_id, zip_path)
    return RedirectResponse(url=f"/exports/{export_id}", status_code=303)


@app.get("/exports/{export_id}")
def export_dashboard(request: Request, export_id: str, category: Optional[str] = None, q: Optional[str] = None):
    conn = get_conn()
    export = conn.execute("SELECT * FROM exports WHERE id = ?", (export_id,)).fetchone()
    if not export:
        conn.close()
        raise HTTPException(status_code=404, detail="Export not found")

    conv_query = """
    SELECT c.*
    FROM conversations c
    WHERE c.export_id = ?
    """
    params = [export_id]

    if category:
        conv_query += """
        AND EXISTS (
            SELECT 1 FROM conversation_categories cc
            JOIN categories cat ON cat.id = cc.category_id
            WHERE cc.conversation_id = c.id AND cat.slug = ? AND cat.export_id = ?
        )
        """
        params.extend([category, export_id])

    if q:
        conv_query += " AND EXISTS (SELECT 1 FROM messages m WHERE m.conversation_id = c.id AND lower(m.content_text) LIKE ?)"
        params.append(f"%{q.lower()}%")

    conv_query += " ORDER BY COALESCE(c.conversation_date, c.created_at) DESC"
    conversations = conn.execute(conv_query, tuple(params)).fetchall()

    conv_ids = [row["id"] for row in conversations]
    messages_by_conv = {}
    if conv_ids:
        placeholders = ",".join("?" for _ in conv_ids)
        rows = conn.execute(
            f"SELECT * FROM messages WHERE conversation_id IN ({placeholders}) ORDER BY created_at ASC",
            tuple(conv_ids),
        ).fetchall()
        for row in rows:
            messages_by_conv.setdefault(row["conversation_id"], []).append(row)

    categories = conn.execute(
        """
        SELECT cat.id, cat.name, cat.slug, COUNT(cc.conversation_id) AS conversation_count
        FROM categories cat
        LEFT JOIN conversation_categories cc ON cc.category_id = cat.id
        WHERE cat.export_id = ?
        GROUP BY cat.id
        ORDER BY cat.name ASC
        """,
        (export_id,),
    ).fetchall()

    assets = conn.execute(
        "SELECT * FROM assets WHERE export_id = ? ORDER BY created_at DESC", (export_id,)
    ).fetchall()
    images = [a for a in assets if a["asset_type"] == "image"]
    audios = [a for a in assets if a["asset_type"] == "audio"]
    files = [a for a in assets if a["asset_type"] == "file"]

    conv_categories = conn.execute(
        """
        SELECT cc.conversation_id, cat.id as category_id, cat.name, cat.slug
        FROM conversation_categories cc
        JOIN categories cat ON cat.id = cc.category_id
        JOIN conversations c ON c.id = cc.conversation_id
        WHERE c.export_id = ?
        ORDER BY cat.name
        """,
        (export_id,),
    ).fetchall()
    cat_by_conv = {}
    for row in conv_categories:
        cat_by_conv.setdefault(row["conversation_id"], []).append(row)

    conn.close()
    return templates.TemplateResponse(
        "export.html",
        {
            "request": request,
            "export": export,
            "conversations": conversations,
            "messages_by_conv": messages_by_conv,
            "categories": categories,
            "cat_by_conv": cat_by_conv,
            "images": images,
            "audios": audios,
            "files": files,
            "selected_category": category,
            "query": q or "",
        },
    )


@app.post("/exports/{export_id}/categories/{category_id}/rename")
def rename_category(export_id: str, category_id: str, name: str = Form(...)):
    conn = get_conn()
    category = conn.execute(
        "SELECT * FROM categories WHERE id = ? AND export_id = ?", (category_id, export_id)
    ).fetchone()
    if not category:
        conn.close()
        raise HTTPException(status_code=404, detail="Category not found")

    conn.execute(
        "UPDATE categories SET name = ?, slug = ?, is_system = 0 WHERE id = ?",
        (name, slugify(name), category_id),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/exports/{export_id}", status_code=303)


@app.post("/exports/{export_id}/conversations/{conversation_id}/move")
def move_conversation(export_id: str, conversation_id: str, category_id: Optional[str] = Form(None), new_category: Optional[str] = Form(None)):
    conn = get_conn()
    conv = conn.execute(
        "SELECT id FROM conversations WHERE id = ? AND export_id = ?", (conversation_id, export_id)
    ).fetchone()
    if not conv:
        conn.close()
        raise HTTPException(status_code=404, detail="Conversation not found")

    target_category_id = category_id
    if new_category:
        target_category_id = ensure_category(conn, export_id, new_category, is_system=0)

    if not target_category_id:
        conn.close()
        raise HTTPException(status_code=400, detail="Category is required")

    conn.execute("DELETE FROM conversation_categories WHERE conversation_id = ?", (conversation_id,))
    conn.execute(
        """
        INSERT OR IGNORE INTO conversation_categories (conversation_id, category_id, source, confidence, created_at)
        VALUES (?, ?, 'manual', NULL, ?)
        """,
        (conversation_id, target_category_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()
    return RedirectResponse(url=f"/exports/{export_id}", status_code=303)
