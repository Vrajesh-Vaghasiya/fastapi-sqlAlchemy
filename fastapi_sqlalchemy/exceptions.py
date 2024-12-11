from typing import Any, Dict, Optional
from fastapi import HTTPException


class BaseHTTPException(HTTPException):
    def __init__(
            self,
            status_code: int,
            error_code: int,
            detail: Any = None,
            headers: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(status_code=status_code, detail=detail)
        self.headers = headers
        self.error_code = error_code
