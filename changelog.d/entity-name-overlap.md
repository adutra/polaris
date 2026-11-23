### Fixes

- A name within a namespace now maps to at most one entity. Creating, registering or renaming a
  table, view or generic table to a name already used by a nested namespace, or creating a
  namespace named like an existing table, view or generic table, is rejected with
  `409 Conflict`. Previously, such collisions were only rejected incidentally by the location
  overlap check (`403 Forbidden`), or went through unchecked for generic tables. Existing
  collisions are not affected.
