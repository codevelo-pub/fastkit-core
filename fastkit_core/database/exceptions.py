
class InvalidCursorError(Exception):
    """
    Raised when a cursor token cannot be decoded.

    Causes: token expired, token not found in backend, token forged or malformed.
    """
    pass