from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from endpoints import teachers

description = """
This API helps interact with the school database.

It allows to:

👉 **List all teachers, students and courses of the school**

👉 **Add new enrolled teacher or students**

👉 **Add new course**

👉 **...**

Further Informations are below 👇
"""

app = FastAPI(
    title="School Management System API",
    description=description,
    summary="🎯 School Management System API to interact with the school mangement databases",
    version="0.0.1",
    terms_of_service="tos",
    contact= {"name": "FD",
              "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
              "email": "fd@fakeemail.com"},
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
)

templates = Jinja2Templates(directory="templates")

app.include_router(teachers.router)

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def home(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})