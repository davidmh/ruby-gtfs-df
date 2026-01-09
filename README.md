# ruby-gtfs-df

[![Tests](https://github.com/davidmh/ruby-gtfs-df/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/davidmh/ruby-gtfs-df/actions/workflows/tests.yml) [![Gem Version](https://badge.fury.io/rb/gtfs_df.svg)](https://badge.fury.io/rb/gtfs_df)

A ruby gem to manipulate [GTFS] feeds using DataFrames using [Polars] ([ruby-polars])

This project was created to bring the power of [partridge] to ruby.

> **⚠️ Warning:** This gem is not ready for production use. It is currently in active development and the API may change without notice.

## Installation

Install the gem and add to the application's Gemfile by executing:

```bash
bundle add gtfs_df
```

If bundler is not being used to manage dependencies, install the gem by executing:

```bash
gem install gtfs_df
```

## Usage

### Loading a GTFS feed

```ruby
require 'gtfs_df'

# Load from a zip file
feed = GtfsDf::Reader.load_from_zip('path/to/gtfs.zip')

# Or, load from a directory
feed = GtfsDf::Reader.load_from_dir('path/to/gtfs_dir')

# Parse times as seconds since midnight instead of string
feed = GtfsDf::Reader.load_from_dir('path/to/gtfs_dir', parse_times: true)

# Access dataframes for each GTFS file
puts feed.agency.head
puts feed.routes.head
puts feed.trips.head
puts feed.stop_times.head
puts feed.stops.head
```

### Filtering feeds

The library supports filtering feeds by any field in any table. The filter automatically cascades through the dependency graph to ensure referential integrity.

```ruby
# Filter by agency
filtered_feed = feed.filter({ 'agency' => { 'agency_id' => 'MTA' } })

# Filter by route
filtered_feed = feed.filter({ 'routes' => { 'route_id' => ['1', '2', '3'] } })

# Filter by a service
filtered_feed = feed.filter({ 'calendar' => { 'service_id' => 'WEEKDAY' } })

# Multiple filters
filtered_feed = feed.filter({
  'agency' => { 'agency_id' => 'MTA' },
  'routes' => { 'route_type' => 1 } # Filter to subway routes
})
```

When you filter by a field, the library automatically:
1. Filters the specified table
2. Cascades related tables following foreign key relationships
3. Keeps only the data that maintains referential integrity

For example, filtering by `agency_id` will automatically filter routes, trips, stop_times, and stops to only include data for that agency.

By default gtfs_df treats trips as the atomic unit of GTFS. Therefore, if we
filter to one stop referenced by TripA, we will preserve _all stops_ referenced
by TripA.

To avoid this behavior, you can pass the `filter_only_children` param. In this case, only the children of the specified filter will be pruned and trip integrity will not be maintained. In the below example, stop 1 and related stop_times will be pruned.

```ruby
filtered_feed = feed.filter({ 'stop' => { 'stop_id' => ['1'] } }, filter_only_children: true)
```


### Writing filtered feeds

```ruby
# Write to a new zip file
GtfsDf::Writer.write_to_zip(filtered_feed, 'output/filtered_gtfs.zip')

# Write to a directory
GtfsDf::Writer.write_to_dir(filtered_feed, 'output/filtered_gtfs')
```

### Example: Split feed by agency

See [examples/split-by-agency](examples/split-by-agency) for a complete example that splits a multi-agency GTFS feed into separate files per agency.

## Development

### Environment

This project manages its development environment with nix, specifically [devenv].

After checking out the repo:

- Install devenv: https://devenv.sh/getting-started/

- To enable the environment you can either:
    - Use [direnv] to enable the environment as soon as you enter the project's path.
    - Enable it only when you needed by running: `devenv shell`

- Run `bin/setup` to install the gem dependencies.

### Tests

Run `rake spec` to run the tests.

### REPL

You can also run `bin/console` for an interactive prompt that will allow you to experiment.

## Release process

1. `bin/bump-version`

- Bumps the version in `lib/gtfs_df/version.rb`
- Updates the `CHANGELOG.md` using the git log since the last version
- Creates and push a new release branch with those changes
- Creates a PR for that release

2. `bin/create-tag`

Creates and pushes the git tag for the release. That will trigger the GitHub action: `.github/workflows/publish.yml` to publish to RubyGems.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/davidmh/ruby-gtfs_df.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

[GTFS]: https://gtfs.org/
[Polars]: https://pola.rs/
[ruby-polars]: https://github.com/ankane/ruby-polars
[partridge]: https://github.com/remix/partridge
[devenv]: https://devenv.sh
[direnv]: https://direnv.net
