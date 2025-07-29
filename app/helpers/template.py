from fastapi.templating import Jinja2Templates

# Set global template root for all templates
templates = Jinja2Templates(directory="app/templates")
