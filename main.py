"""OneDrive to Amazon Photos sync — list Amazon Photos, compare with OneDrive."""
from pathlib import Path

# Load .env from project root so ONEDRIVE_CLIENT_ID, AMAZON_* etc. can be set there
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_path)

from list_amazon_photos import app

if __name__ == "__main__":
    app()
