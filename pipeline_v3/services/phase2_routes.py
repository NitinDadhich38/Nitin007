from pathlib import Path
from typing import Dict, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from .export_service import company_export_response, create_bulk_excel_export, get_export_job
from .filing_availability import check_company_filing
from .index_registry import companies_for_index, load_indices


def register_phase2_routes(app: FastAPI) -> None:
    @app.get("/api/indices")
    def list_indices():
        return load_indices()

    @app.get("/api/indices/{index_code}/companies")
    def get_index_companies(index_code: str):
        index_code = index_code.upper()
        indices = load_indices()
        if index_code not in indices:
            raise HTTPException(status_code=404, detail=f"Index {index_code} not found")
        return companies_for_index(index_code)

    @app.get("/api/companies/{symbol}/filings")
    def get_company_filing_status(
        symbol: str,
        period_end: str = Query("2026-03-31", description="ISO period end date, e.g. 2026-03-31"),
    ):
        return check_company_filing(symbol, period_end)

    @app.post("/api/filings/check")
    def check_filings(payload: Dict = Body(...)):
        period_end = payload.get("period_end") or payload.get("periodEnd") or "2026-03-31"
        symbols = payload.get("symbols") or []
        if not symbols:
            index_code = (payload.get("index_code") or payload.get("indexCode") or "NIFTY100").upper()
            symbols = [company["symbol"] for company in companies_for_index(index_code)]
        return {
            "period_end": period_end,
            "results": [check_company_filing(symbol, period_end) for symbol in symbols],
        }

    @app.get("/api/companies/{symbol}/export")
    def export_company(
        symbol: str,
        format: str = Query("xlsx", alias="format"),
        period_end: str = Query("2026-03-31"),
        result_type: str = Query("all", enum=["all", "consolidated", "standalone"]),
    ):
        return company_export_response(symbol, format, period_end, result_type)

    @app.post("/api/admin/exports/bulk")
    def create_bulk_export(payload: Dict = Body(default={})):
        index_code = (payload.get("index_code") or payload.get("indexCode") or "NIFTY100").upper()
        period_end = payload.get("period_end") or payload.get("periodEnd") or "2026-03-31"
        return create_bulk_excel_export(index_code, period_end)

    @app.get("/api/admin/exports/{job_id}")
    def get_bulk_export(job_id: str):
        return get_export_job(job_id)

    @app.get("/api/admin/exports/{job_id}/download")
    def download_bulk_export(job_id: str):
        job = get_export_job(job_id)
        zip_path = Path(job["zip_path"])
        if not zip_path.exists():
            raise HTTPException(status_code=404, detail="Export ZIP not found")
        return FileResponse(zip_path, media_type="application/zip", filename=zip_path.name)
