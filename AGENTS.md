# AGENTS.md

Guidance for coding agents working in this repository.

## Current planning context

Before implementing GitHub issue #3 or related ingester database isolation work, read:

- `doc/database-isolation-plan.md`

That file captures the agreed design from the planning session, including path semantics, staging/public isolation invariants, export/publish mechanics, lifecycle behavior, and test coverage.

Related GitHub issues:

- #3 Database isolation is doubleplus ungood
- #4 Add ingester staging-directory config and path safety validation
- #5 Implement clean SQLite snapshot export and atomic public publish
- #6 Rework DBCache around durable staging and public import isolation
- #7 Add staging recovery, throttled export, and idle finalization
- #8 Complete tests and operator docs for database isolation
