# litscout/client/__init__.py

from __future__ import annotations

from flask import Flask
from server.api import LitScoutAPI


def create_app() -> Flask:
    """
    Flask application factory for the LitScout UI.
    """
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "change-me-in-production"

    # Single shared LitScoutAPI instance
    app.litscout_api = LitScoutAPI()
    app.litscout_api.start_database()

    # Register blueprints
    from client.views import main_bp
    app.register_blueprint(main_bp)

    return app
