# litscout/client/__init__.py

from __future__ import annotations

from flask import Flask
from litscout.server.api import LitScoutAPI


def create_app() -> Flask:
    """
    Flask application factory for the LitScout UI.
    """
    app = Flask(__name__)
    app.config["SECRET_KEY"] = "change-me-in-production"

    # Single shared LitScoutAPI instance
    app.litscout_api = LitScoutAPI()

    # Register blueprints
    from litscout.client.views import main_bp
    app.register_blueprint(main_bp)

    return app
