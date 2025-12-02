import os
from datetime import datetime

import fitz  # PyMuPDF
import requests
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi import HTTPException
from pydantic import BaseModel 
import httpx

# ========= APP FASTAPI =========

app = FastAPI()

# ========= CONFIG =========

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

JOBS_TABLE = "Jobs"
CANDIDATES_TABLE = "Candidates"

MAKE_WEBHOOK_URL = os.getenv("MAKE_WEBHOOK_URL")

class TriggerAnalysisPayload(BaseModel):
    job_id: str

@app.post("/trigger-analysis")
async def trigger_analysis(payload: TriggerAnalysisPayload):
    if not MAKE_WEBHOOK_URL:
        raise HTTPException(
            status_code=500,
            detail="MAKE_WEBHOOK_URL is not configured on the server.",
        )
    # 1️⃣ Récupérer la description du job dans Airtable
    _check_airtable_env()

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{JOBS_TABLE}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    }
    params = {
        "filterByFormula": f'{{job_id}} = "{payload.job_id}"',
        "pageSize": 1,
    }

    try:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print("Error fetching job from Airtable:", e)
        raise HTTPException(
            status_code=502,
            detail="Error while fetching job from Airtable.",
        )

    records = data.get("records", [])
    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No job found in Airtable for job_id={payload.job_id}",
        )

    job_fields = records[0].get("fields", {})
    description_raw = job_fields.get("description_raw", "")

    # 2️⃣ Appeler le webhook Make avec job_id + description_raw
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                MAKE_WEBHOOK_URL,
                json={
                    "job_id": payload.job_id,
                    "description_raw": description_raw,
                },
            )
        resp.raise_for_status()
    except Exception as e:
        # Log pour Render
        print("Error calling Make webhook:", e)
        raise HTTPException(
            status_code=502,
            detail="Error while calling Make webhook.",
        )

    return {"status": "ok"}


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

def _check_airtable_env():
    if not AIRTABLE_TOKEN or not AIRTABLE_BASE_ID:
        raise RuntimeError(
            "Les variables d'environnement AIRTABLE_TOKEN et AIRTABLE_BASE_ID "
            "doivent être définies pour utiliser Airtable."
        )


def airtable_create_record(table: str, fields: dict) -> dict:
    """
    Crée un enregistrement dans Airtable.
    """
    _check_airtable_env()

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


def airtable_update_record(table: str, record_id: str, fields: dict) -> dict:
    """
    Met à jour un enregistrement dans Airtable (PATCH).
    """
    _check_airtable_env()

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}/{record_id}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"fields": fields}
    r = requests.patch(url, json=payload, headers=headers)

    if not r.ok:
        print("Airtable update error:", r.status_code, r.text)
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
    description: str = Form(...),
):
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

    _check_airtable_env()

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
            candidates.append(
                {
                    "id": rec.get("id"),
                    "file_name": fields.get("file_name"),
                    "score": fields.get("score"),
                    "decision": fields.get("decision"),
                    "analysis_status": fields.get("analysis_status"),
                    "analysis_explanation": fields.get("analysis_explanation"),
                }
            )

        offset = data.get("offset")
        if not offset:
            break
# On ne filtre plus ici, on renvoie vraiment tous les candidats
# (pending et done)
candidates.sort(key=lambda c: (c.get("score") or 0), reverse=True)

return {"candidates": candidates}


# ========= UPDATE DECISION =========

class UpdateDecisionPayload(BaseModel):
  candidate_id: str
  decision: str  # "yes" ou "no" côté front


@app.post("/update-decision")
def update_decision(payload: UpdateDecisionPayload):
    """
    Met à jour la décision d'un candidat dans Airtable.
    candidate_id = record.id dans la table Candidates
    decision = "yes" ou "no" (côté front)
    """
    if payload.decision not in ["yes", "no"]:
        raise HTTPException(
            status_code=400,
            detail="decision must be 'yes' or 'no'",
        )

    # Si dans Airtable tu veux enregistrer "OUI"/"NON", adapte ici :
    airtable_value = "OUI" if payload.decision == "yes" else "NON"

    try:
        record = airtable_update_record(
            CANDIDATES_TABLE,
            payload.candidate_id,
            {"decision": airtable_value},
        )
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "ok", "airtable_id": record.get("id")}
