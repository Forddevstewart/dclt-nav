import re
from flask import request, session
from flask_login import current_user
from .models import get_db

_EVENT_MAP = [
    (r"^/api/overview$",                       "overview"),
    (r"^/api/parcels$",                         "parcels.list"),
    (r"^/api/parcels/[^/]+$",                  "parcel.view"),
    (r"^/api/documents$",                       "docs.list"),
    (r"^/api/documents/[^/]+/[^/]+/pdf$",      "doc.pdf"),
    (r"^/api/documents/[^/]+/[^/]+/rod$",      "doc.rod"),
    (r"^/api/documents/[^/]+/[^/]+/[^/]+/history$", "adj.history"),
    (r"^/api/documents/[^/]+/[^/]+$",          "doc.view"),
    (r"^/api/adjudications/by_keyword/",        "adj.by_kw"),
    (r"^/api/adjudications/document/",          "adj.doc"),
    (r"^/api/adjudications$",                   "adjudication"),
    (r"^/api/admin/users/\d+/password$",        "admin.pw_change"),
    (r"^/api/admin/users$",                     "admin.users"),
    (r"^/api/admin/usage$",                     "admin.usage"),
]


def classify(path: str) -> str:
    for pattern, label in _EVENT_MAP:
        if re.match(pattern, path):
            return label
    return "api"


def log_event(event_type: str, api_call: str = None, details: str = None):
    try:
        uid = current_user.id if current_user.is_authenticated else None
        uname = current_user.username if current_user.is_authenticated else None
        sid = session.get("_sid", "")
        ip = request.remote_addr
        ua = (request.headers.get("User-Agent") or "")[:300]
        db = get_db()
        db.execute(
            """INSERT INTO usage_log
               (user_id, username, session_id, event_type, api_call, details, ip, user_agent)
               VALUES (?,?,?,?,?,?,?,?)""",
            (uid, uname, sid, event_type, api_call or request.path, details, ip, ua),
        )
        db.commit()
        db.close()
    except Exception:
        pass
