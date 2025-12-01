import os
from datetime import datetime

import fitz  # PyMuPDF
import requests
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi import HTTPException

# ========= APP FASTAPI =========

app = FastAPI()

# ========= CONFIG =========

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

JOBS_TABLE = "Jobs"
CANDIDATES_TABLE = "Candidates"


# ========= ROUTES DE DEBUG =========

@app.get("/debug-env")
def debug_env():
    """
    Route de debug pour vérifier ce que voit Render.
    NE L'UTILISE QUE TEMPORAIREMENT.
    """
    return {
        "AIRTABLE_TOKEN_prefix": AIRTABLE_TOKEN[:4] if AIRTABLE_TOKEN else None,
        "AIRTABLE_TOKEN_len": len(AIRTABLE_TOKEN) if AIRTABLE_TOKEN else None,
        "AIRTABLE_BASE_ID": AIRTABLE_BASE_ID,
    }


@app.get("/debug-airtable")
def debug_airtable():
    """
    Teste le token Airtable utilisé par Render en appelant l'endpoint /meta/whoami.
    """
    url = "https://api.airtable.com/v0/meta/whoami"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    }
    r = requests.get(url, headers=headers)
    content_type = r.headers.get("Content-Type", "")

    return {
        "status_code": r.status_code,
        "body": r.json() if content_type.startswith("application/json") else r.text,
    }


# ========= CORS =========

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ========= FONCTIONS UTILITAIRES =========

def airtable_create_record(table: str, fields: dict) -> dict:
    """
    Crée un enregistrement dans Airtable.
    """
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        raise RuntimeError(
            "Les variables d'environnement AIRTABLE_TOKEN et AIRTABLE_BASE_ID "
            "doivent être définies pour utiliser Airtable."
        )

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"fields": fields}
    r = requests.post(url, json=payload, headers=headers)

    if not r.ok:
        print("Airtable error:", r.status_code, r.text)
        raise RuntimeError(f"Airtable error {r.status_code}: {r.text}")

    return r.json()


def extract_text_from_pdf_bytes(file_bytes: bytes) -> str:
    """
    Extrait le texte d'un PDF non scanné.
    """
    pdf = fitz.open(stream=file_bytes, filetype="pdf")
    text = ""
    for page in pdf:
        text += page.get_text()
    pdf.close()
    return text


# ========= ENDPOINTS MÉTIER =========

@app.post("/create-job")
async def create_job(
    title: str = Form(None), 
    description: str = Form(...)):
    """
    Crée un job dans Airtable.
    """
    job_id = f"JOB-{int(datetime.utcnow().timestamp())}"

    record = airtable_create_record(
        JOBS_TABLE,
        {
            "job_id": job_id,
            "description_raw": description,
        },
    )

    return {
        "status": "ok",
        "job_id": job_id,
        "airtable_id": record["id"],
    }


@app.post("/upload-cv")
async def upload_cv(job_id: str = Form(...), file: UploadFile = File(...)):
    """
    Reçoit un PDF + job_id, extrait le texte, crée un candidate dans Airtable.
    """
    file_bytes = await file.read()
    text = extract_text_from_pdf_bytes(file_bytes)

    record = airtable_create_record(
        CANDIDATES_TABLE,
        {
            "job_id": job_id,
            "file_name": file.filename,
            "cv_text_raw": text,
            "analysis_status": "pending",
        },
    )

    return {
        "status": "ok",
        "candidate_id": record["id"],
    }

@app.get("/results")
def get_results(job_id: str):
    """
    Retourne la liste des candidats pour un job_id donné,
    avec score, décision, etc.
    """
    if not job_id:
        raise HTTPException(status_code=400, detail="job_id is required")

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{CANDIDATES_TABLE}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    }

    params = {
        "filterByFormula": f'{{job_id}} = "{job_id}"',
        "pageSize": 100,
    }

    candidates = []
    offset = None

    while True:
        if offset:
            params["offset"] = offset

        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()

        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            candidates.append({
                "id": rec.get("id"),
                "file_name": fields.get("file_name"),
                "score": fields.get("score"),
                "decision": fields.get("decision"),
                "analysis_status": fields.get("analysis_status"),
                "analysis_explanation": fields.get("analysis_explanation"),
            })

        offset = data.get("offset")
        if not offset:
            break

    # On garde ceux qui sont vraiment analysés
    done = [
        c for c in candidates
        if c.get("analysis_status") == "done" and c.get("score") is not None
    ]

    # On les trie par score décroissant
    done.sort(key=lambda c: c.get("score", 0), reverse=True)

    return {"candidates": done}
