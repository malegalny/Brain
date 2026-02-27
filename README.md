# Brain

Brain is a web application that lets you upload ChatGPT export ZIP files and organize all conversations and media in one place.

## What is implemented

- Upload ChatGPT export ZIP files.
- Extract and parse `conversations.json`.
- Persist conversations + messages as structured records in SQLite.
- Extract and store media/attachments from the ZIP.
- Per-export dashboard with:
  - Conversations list sorted by date
  - Image gallery
  - Audio list with playback controls
  - Attachments table with download links
- Automatic category detection (`housing court case`, `dog`, `restaurant`, fallback `uncategorized`).
- Category sidebar with conversation counts.
- Category filtering.
- Rename categories.
- Move conversations between categories (existing or new category).
- Search inside conversation message text.
- Export isolation (all data scoped to one `export_id`; uploads are separate).

## Tech stack

- FastAPI
- Jinja2 templates
- SQLite
- Local filesystem storage under `storage/`

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --reload
```

Open: `http://127.0.0.1:8000`

## Project layout

```text
app.py
requirements.txt
templates/
  index.html
  export.html
storage/
```

## Notes

- ZIP parsing is implemented synchronously right after upload for simplicity.
- Asset-to-message linking is best-effort (filename mention matching in message text).
- This is an MVP foundation and can be upgraded to background workers + PostgreSQL/S3.
