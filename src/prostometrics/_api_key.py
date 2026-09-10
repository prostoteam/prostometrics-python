"""Recognising which kind of key was configured.

Client keys carry a marker that server keys never do. Both are minted from the
same settings screen and look alike at a glance, so pasting the wrong one is an
ordinary mistake rather than an exotic one.
"""

CLIENT_KEY_MARKER = "_pk_"


def looks_like_client_key(api_key: str) -> bool:
    """Report whether the configured key is a client key.

    This client can never authenticate with one. Only the marker is inspected:
    the key is otherwise opaque, and guessing harder would risk rejecting a
    valid key the service starts issuing later.
    """
    return CLIENT_KEY_MARKER in api_key.strip()


def refusal_hint(api_key: str) -> str:
    """Explain a refusal the key's own shape already accounts for.

    Empty when the key looks like the server key it should be, because then the
    refusal says nothing more specific than that the key is wrong.
    """
    if not looks_like_client_key(api_key):
        return ""
    return f' \u2014 this is a client key (it contains "{CLIENT_KEY_MARKER}"); server-side clients need a server key'
