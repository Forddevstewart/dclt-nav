import os
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from .models import get_db

bp = Blueprint("auth", __name__)
login_manager = LoginManager()


class User(UserMixin):
    def __init__(self, id, username, role="user"):
        self.id = id
        self.username = username
        self.role = role


@login_manager.user_loader
def load_user(user_id):
    db = get_db()
    row = db.execute(
        "SELECT id, username, role FROM users WHERE id = ?", (user_id,)
    ).fetchone()
    db.close()
    if row:
        return User(row["id"], row["username"], row["role"])
    return None


@login_manager.unauthorized_handler
def unauthorized():
    return redirect(url_for("auth.login", next=request.path))


@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("routes.index"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        db = get_db()
        row = db.execute(
            "SELECT id, username, password_hash, role FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if row and check_password_hash(row["password_hash"], password):
            db.execute(
                "UPDATE users SET last_login = datetime('now') WHERE id = ?",
                (row["id"],),
            )
            db.commit()
            db.close()
            login_user(User(row["id"], row["username"], row["role"]), remember=True)
            from .usage import log_event
            log_event("login", api_call="/login", details=f"user={username}")
            return redirect(request.args.get("next") or url_for("routes.index"))
        db.close()
        flash("Invalid username or password.")
    return render_template("login.html")


@bp.route("/logout")
def logout():
    from .usage import log_event
    log_event("logout", api_call="/logout")
    logout_user()
    return redirect(url_for("routes.index"))


def ensure_ford(db_path: str) -> None:
    """Create the ford admin user if absent; promote to admin if role is wrong."""
    import sqlite3
    pw = os.environ.get("FORD_PASSWORD", "ford")
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT id, role FROM users WHERE username = 'ford'"
    ).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
            ("ford", generate_password_hash(pw), "admin"),
        )
    elif row[1] != "admin":
        conn.execute("UPDATE users SET role = 'admin' WHERE username = 'ford'")
    conn.commit()
    conn.close()
