import os
from dotenv import load_dotenv

# Φορτώνει τις τιμές από το .env αρχείο
load_dotenv()

# Παίρνουμε τις μεταβλητές μία-μία
user = os.getenv("DB_USER")
pw = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db = os.getenv("DB_NAME")

# Φτιάχνουμε το έτοιμο URL για την SQLAlchemy
DATABASE_URL = f"postgresql://{user}:{pw}@{host}:{port}/{db}"