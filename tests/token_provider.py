"""``tokenProvider`` functions for tests/test_credentials.py, named as ``token_provider:<name>``
(pytest puts tests/ on sys.path)."""

from types import SimpleNamespace

calls = 0


def token() -> str:
    """A new token per call, numbered: which connection got which shows."""
    global calls
    calls += 1
    return f"eyJ0eXAi.token-{calls}"


def failing() -> str:
    raise OSError("no route to login.microsoftonline.com")


def access_token() -> object:
    """azure-identity's AccessToken instead of its ``.token``: a common slip."""
    return SimpleNamespace(token="eyJ0eXAi.wrapped", expires_on=0)


not_a_function = "eyJ0eXAi.constant"
