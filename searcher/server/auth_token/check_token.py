from datetime import datetime, timedelta

import jwt

from settings import SECRET_KEY, ALGORITHM


def check_jwt_token(token: str):
    try:
        decoded = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if decoded.get("date") and decoded.get("date") == datetime.now().strftime(
            "%Y-%m-%d"
        ):
            return True
    except jwt.exceptions.ExpiredSignatureError:
        pass
    return False


def gen_token():
    to_encode = {
        "exp": (datetime.now() + timedelta(hours=1)).timestamp(),
        "date": datetime.now().strftime("%Y-%m-%d"),
    }
    token = jwt.encode(payload=to_encode, algorithm=ALGORITHM, key=SECRET_KEY)
    return token


if __name__ == "__main__":
    print(gen_token())