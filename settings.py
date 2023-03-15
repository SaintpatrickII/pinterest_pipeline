import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

AWS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET = os.environ.get("AWS_SECRET_KEY")
