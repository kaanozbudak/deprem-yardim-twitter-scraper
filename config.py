from environs import Env

env = Env()
# Read .env into os.environ
env.read_env()

X_API_KEY = env.str("X_API_KEY", "")
app_name = "TwitterScraper"