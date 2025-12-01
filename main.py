import fitz  # PyMuPDF
import requests
from datetime import datetime

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware

# ========= CONFIG =========

AIRTABLE_TOKEN = "patBFA1fDASEFqUbX.9e79e2e4ca92f8c96710ffc5e05c32e748510140af0a827c1209c312c828e79b"
AIRTABLE_BASE_ID = "appfowJWq52KZuxop"

JOBS_TABLE = "Jobs"
CANDIDATES_TABLE = "Candidates"

# ========= APP FASTAPI =========

app = FastAPI()

# CORS pour permettre au frontend Vercel d'appeler l'API plus tard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def airtable_create_record(table: str, fields: dict) -> dict:
    """
    Crée un enregistrement dans Airtable.
    """
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"fields": fields}
    r = requests.post(url, json=payload, headers=headers)
    r.raise_for_status()
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


@app.post("/create-job")
async def create_job(title: str = Form(...), description: str = Form(...)):
    """
    Crée un job dans Airtable.
    """
    job_id = f"JOB-{int(datetime.utcnow().timestamp())}"

    record = airtable_create_record(
        JOBS_TABLE,
        {
            "job_id": job_id,
            "title": title,
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
