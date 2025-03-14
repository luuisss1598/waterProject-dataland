from dotenv import load_dotenv, find_dotenv
import os 

"""
1. find the path where the .env file is stored and load
2. load the variables using load_dotenv()
3. use os to get the given env needed
"""

# pass variable needed
find_env_path: str = find_dotenv();
load_dotenv(find_env_path, override=True);

def get_env(requested_env_variable: str) -> str:
    get_env_variable = os.getenv(requested_env_variable)

    return get_env_variable

