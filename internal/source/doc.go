package source

// KAN-56 boundary:
// - HC SS is owner and must authorize file_id before Stat/OpenRange.
// - Gateway never trusts addresses from UI/TV and never forwards file paths.
// - Local root mappings must be explicitly administered on Gateway's host.
// - REMOTE_PREFETCH stores bytes on disk in KAN-58, NOT in this resolver.
// - REMOTE_BLOCK_STREAM uses bounded ranged WS reads in KAN-59.
// - LOCAL_DIRECT reader with fresh path verification is implemented in KAN-57.
