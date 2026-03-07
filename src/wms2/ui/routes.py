from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

templates_dir = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

ui_router = APIRouter(prefix="/ui", tags=["ui"])


@ui_router.get("/")
async def dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html", {
        "api_prefix": request.app.state.settings.api_prefix,
        "active_page": "dashboard",
    })


@ui_router.get("/requests")
async def request_list(request: Request):
    return templates.TemplateResponse(request, "requests.html", {
        "api_prefix": request.app.state.settings.api_prefix,
        "active_page": "requests",
    })


@ui_router.get("/requests/{request_name:path}")
async def request_detail(request_name: str, request: Request):
    return templates.TemplateResponse(request, "request_detail.html", {
        "api_prefix": request.app.state.settings.api_prefix,
        "active_page": "requests",
        "request_name": request_name,
    })


@ui_router.get("/workflows/{workflow_id}")
async def workflow_detail(workflow_id: str, request: Request):
    return templates.TemplateResponse(request, "workflow_detail.html", {
        "api_prefix": request.app.state.settings.api_prefix,
        "active_page": "requests",
        "workflow_id": workflow_id,
    })


@ui_router.get("/dags/{dag_id}")
async def dag_detail(dag_id: str, request: Request):
    return templates.TemplateResponse(request, "dag_detail.html", {
        "api_prefix": request.app.state.settings.api_prefix,
        "active_page": "requests",
        "dag_id": dag_id,
    })


@ui_router.get("/monitoring")
async def monitoring_page(request: Request):
    return templates.TemplateResponse(request, "monitoring.html", {
        "api_prefix": request.app.state.settings.api_prefix,
        "active_page": "monitoring",
    })


@ui_router.get("/import")
async def import_page(request: Request):
    return templates.TemplateResponse(request, "import.html", {
        "api_prefix": request.app.state.settings.api_prefix,
        "active_page": "import",
    })


@ui_router.get("/settings")
async def settings_page(request: Request):
    return templates.TemplateResponse(request, "settings.html", {
        "api_prefix": request.app.state.settings.api_prefix,
        "active_page": "settings",
    })
