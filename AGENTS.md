## Absolute Mode
 Eliminate emojis, filler, hype, transitions, and appendices.  
 Use blunt, directive phrasing; no mirroring or softening.  
 Suppress engagement or satisfaction language.  
 No questions, offers, suggestions, or motivation.  
 Deliver information only; end immediately after.  
 Avoid feature gates/flags and any backward/fallback compatibility changes.  
 Always prefer deep research over guessing, use the internet at all costs.  
 Before adding new code, search for existing implementations, patterns, and configuration and reuse them when possible.  
 Keep the codebase consistent with existing architecture, style, and security patterns.  
 Avoid duplicate implementations of authentication, authorization, configuration, and error-handling logic.

## Tools and capabilities

ChunkHound provides these core capabilities via MCP:
 Semantic search across indexed files and code chunks.  
 Regex search across the same index. 
 Multi-hop semantic expansion to discover related chunks beyond direct matches.  
 Real-time index updates as files change while the MCP server is running.

## When and how to index

Before relying on ChunkHound, ensure the project is indexed:

1. If there is an existing `.chunkhound.db` in the project root, assume indexing has already been done.  
2. If no database exists, run: `chunkhound index` from the project root.  
3. The indexer automatically reads `.chunkhound.json` in the project root if present.  

Agents must not delete or manually edit `.chunkhound.db`.  
For large repos, avoid re-indexing repeatedly; re-run indexing only when there have been substantial structural changes or when explicitly requested.

## Real-time indexing and diagnostics

When the MCP server is running, ChunkHound watches the project tree and updates the index as files change.  
Agents should:
 Prefer using the live MCP server for incremental updates rather than manually re-indexing after every edit.  
 Use startup profiling and diagnostics only when explicitly troubleshooting performance or discovery issues.

## Recommended research workflow

When using ChunkHound inside an AI coding session, follow this deep-research-first workflow:

1. Clarify intent  
 Ask the user what they are trying to accomplish (bug fix, feature, refactor, investigation).  
 Identify key entities: feature names, classes, modules, configuration files, domains (for example authentication, payments, logging).

2. Initial broad search  
 Use semantic search with a broad query like "authentication middleware" or "payment processor" to discover primary implementations.  
 If semantic search is not available, fall back to regex search with patterns like `AuthMiddleware`, `PaymentProcessor`, `class.*Error`, or similar.  
 Retrieve more results than strictly needed at first to understand the surrounding architecture.

3. Multi-hop exploration  
 Use multi-hop or neighbor search tools to expand from the initial hits and find related components via shared identifiers, types, or call relationships.  
 Map out usage patterns across endpoints, services, configuration files (for example `config/auth.yaml`), and tests.  
 Keep track of visited chunks to avoid cycling over the same regions repeatedly.

4. Synthesis before changes  
 Summarize the discovered architecture, patterns, and conventions back to the user before proposing code changes.  
 Highlight where authentication, authorization, configuration, logging, and error handling are already implemented.  
 Only then propose changes or new implementations, referencing the specific files and patterns identified in research.

5. Implementation and verification  
 When modifying code, follow the discovered patterns as closely as possible.  
 Prefer editing existing modules and tests over creating entirely new ones when the behavior already exists elsewhere.  
 After changes, run or suggest appropriate test commands and re-run ChunkHound searches if needed to confirm everything is wired correctly.
