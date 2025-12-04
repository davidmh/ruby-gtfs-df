## [0.4.0] - 2025-12-04

### Added

- allow setting maintain_trip_dependencies=false

### Fixed

- parse stop_lat as float
- add missing agency -> fare_attributes edge
- allow null for fare_rules

### Maintenance

- provide accessor for gtfs_files (utility)
- add yard docs

## [0.3.0] - 2025-12-04

### Added

- keep parent stations linked to used stops

### Fixed

- handle null values
- update lock on version bump

### Maintenance

- reuse load_from_dir logic in reader
- clean up unused method + better comments
## [0.1.0] - 2025-11-10

- Initial release
