from typing import Protocol, Any
from sqlalchemy import Select


class QueryScope(Protocol):
    """
    Protocol for query scopes applied automatically to all repository queries.

    Implement this protocol to add reusable WHERE conditions that are injected
    before every query execution on a repository instance.

    Scopes are composable — multiple scopes can be registered on the same
    repository and each apply() receives the output of the previous one.

    Example:
```python
    class AgencyScope:
        def __init__(self, agency_id: str) -> None:
            self._agency_id = agency_id

        def apply(self, query: Select, model: Any) -> Select:
            return query.where(model.agency_id == self._agency_id)

    # Register on repository
    repo.add_scope(AgencyScope(current_user.agency_id))
```
    """

    def apply(self, query: Select, model: Any) -> Select:
        """
        Apply scope conditions to the query.

        Args:
            query: Current SQLAlchemy Select statement.
            model: SQLAlchemy model class being queried.

        Returns:
            Modified Select statement with scope conditions applied.
        """
        ...