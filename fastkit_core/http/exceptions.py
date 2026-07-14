class FastKitException(Exception):
    """
    Base exception for FastKit.

    All FastKit exceptions inherit from this class.
    Includes status_code and optional errors dict.

    Attributes:
        message: Error message
        status_code: HTTP status code
        errors: Optional error details
    """

    def __init__(
            self,
            message: str,
            status_code: int = 400,
            errors: dict | None = None,
            headers: dict | None = None
    ):
        self.message = message
        self.status_code = status_code
        self.errors = errors
        self.headers = headers
        super().__init__(self.message)

class NotFoundException(FastKitException):
    """Resource not found."""
    def __init__(self, message: str = "Resource not found"):
        super().__init__(message, status_code=404)

class ValidationException(FastKitException):
    """Validation failed."""
    def __init__(self, errors: dict, message: str = "Validation failed"):
        super().__init__(message, status_code=422, errors=errors)

class UnauthorizedException(FastKitException):
    """Not authenticated."""
    def __init__(self, message: str = "Unauthorized"):
        super().__init__(message, status_code=401)

class ForbiddenException(FastKitException):
    """Not authorized."""
    def __init__(self, message: str = "Forbidden"):
        super().__init__(message, status_code=403)

class TooManyRequestsException(FastKitException):
    """Too Many requests"""
    def __init__(self,
                 message: str = "Too Many requests",
                 headers: dict | None = None
                 ):
        super().__init__(message, headers=headers, status_code=429)
