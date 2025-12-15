## [0.6.2] - 2025-12-15

### 🐛 Bug Fixes

- Permit non UTF-8 characters
## [0.6.1] - 2025-12-12

### 🐛 Bug Fixes

- Parse whitespace in column headers

### 📚 Documentation

- Badges

### ⚙️ Miscellaneous Tasks

- Update devenv
- Drop custom changelog parsing
- Bump version to 0.6.1
## [0.6.0] - 2025-12-10

### 🐛 Bug Fixes

- Visit nodes multiple times

### ⚙️ Miscellaneous Tasks

- Bump version to 0.6.0
## [0.5.0] - 2025-12-08

### 🚀 Features

- [**breaking**] Add Feed#filter filter_only_children param

### ⚙️ Miscellaneous Tasks

- Arrange edges so parent is always first
- Build directed graph
- Allow ! in commit messages
- Bump version to 0.5.0
## [0.4.1] - 2025-12-05

### 🚀 Features

- Handle extra whitespace in csvs

### ⚙️ Miscellaneous Tasks

- Remove unreleased section
- Remove unused initializer format
- Bump version to 0.4.1
## [0.4.0] - 2025-12-04

### 🚀 Features

- Allow setting maintain_trip_dependencies=false

### 🐛 Bug Fixes

- Parse stop_lat as float
- Add missing agency -> fare_attributes edge
- Allow null for fare_rules

### ⚙️ Miscellaneous Tasks

- Provide accessor for gtfs_files (utility)
- Add yard docs
- Bump version to 0.4.0
## [0.3.0] - 2025-12-04

### 🚀 Features

- Keep parent stations linked to used stops

### 🐛 Bug Fixes

- Handle null values
- Update lock on version bump

### ⚙️ Miscellaneous Tasks

- Reuse load_from_dir logic in reader
- Clean up unused method + better comments
- Autopublish on release tag push
- Automate release script
- Release tag script
- Bump version to 0.3.0
## [0.2.0] - 2025-12-01

### 🚀 Features

- Add Reader.load_from_dir

### 🐛 Bug Fixes

- Require correct entrypoint
- Cascade empty view filters
- Handle parsing when cols size = schema size
- Parse extraneous columns as strings
- Cascade changes reliably
- Filter with trips as atomic unit
- Remove nonexistent booking_rule association
- Add empty string to null vals

### 📚 Documentation

- Include processing time
- Update gem name

### ⚙️ Miscellaneous Tasks

- Add byebug gem
- Include byebug in spec_helper.rb
- Rearrange filter specs
- Add pending specs for expected behaviors
- [**breaking**] Removes duplicate load_from_dir method (use reader instead)
- Mutate for both filter! and prune!
- Tag version 0.2.0
## [0.1.1] - 2025-11-12

### 🐛 Bug Fixes

- Release workflow

### ⚙️ Miscellaneous Tasks

- Rename namespace to follow ruby conventions
- Bump version
- Remove broken release flow
- Clarify gem status
- Republish version
## [0.1.0] - 2025-11-12

### 📚 Documentation

- Readme and gemspec details
- Time parsing to-do

### ⚙️ Miscellaneous Tasks

- Initial commit
- Make the lock platform agnostic
- Validate commit messages
- Run spec and standard steps separately
- Release flow
- Initial release
