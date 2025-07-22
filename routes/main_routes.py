# routes/main_routes.py

from flask import Blueprint, render_template
from routes.parsers_config import PARSERS
from utils.help_texts import HelpTexts

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    return render_template('parser_tabs.html', parsers=PARSERS)
