# ruby-gtfs-df

A ruby gem to manipulate [GTFS] feeds using DataFrames using [Polars] ([ruby-polars])

This project was created to bring the power of [partridge] to ruby.

## Installation

TODO: Replace `UPDATE_WITH_YOUR_GEM_NAME_IMMEDIATELY_AFTER_RELEASE_TO_RUBYGEMS_ORG` with your gem name right after releasing it to RubyGems.org. Please do not do it earlier due to security reasons. Alternatively, replace this section with instructions to install your gem from git if you don't plan to release to RubyGems.org.

Install the gem and add to the application's Gemfile by executing:

```bash
bundle add UPDATE_WITH_YOUR_GEM_NAME_IMMEDIATELY_AFTER_RELEASE_TO_RUBYGEMS_ORG
```

If bundler is not being used to manage dependencies, install the gem by executing:

```bash
gem install UPDATE_WITH_YOUR_GEM_NAME_IMMEDIATELY_AFTER_RELEASE_TO_RUBYGEMS_ORG
```

## Usage

### Loading a GTFS feed

```ruby
require 'gtfs_df'

# Load from a zip file
feed = GtfsDf::Reader.load_from_zip('path/to/gtfs.zip')

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
filtered_feed = feed.filter('agency' => { 'agency_id' => 'MTA' })

# Filter by route
filtered_feed = feed.filter('routes' => { 'route_id' => ['1', '2', '3'] })

# Filter by a service
filtered_feed = feed.filter('calendar' => { 'service_id' => 'WEEKDAY' })

# Multiple filters
filtered_feed = feed.filter(
  'agency' => { 'agency_id' => 'MTA' },
  'routes' => { 'route_type' => 1 } # Filter to subway routes
)
```

When you filter by a field, the library automatically:
1. Filters the specified table
2. Cascades related tables following foreign key relationships
3. Keeps only the data that maintains referential integrity

For example, filtering by `agency_id` will automatically filter routes, trips, stop_times, and stops to only include data for that agency.

### Writing filtered feeds

```ruby
# Write to a new zip file
GtfsDf::Writer.write_to_zip(filtered_feed, 'output/filtered_gtfs.zip')
```

### Example: Split feed by agency

See [examples/split-by-agency](examples/split-by-agency) for a complete example that splits a multi-agency GTFS feed into separate files per agency.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## TODO

- [ ] Time parsing
    Just like partridge, we should parse Time as seconds since midnight. There's a draft in `lib/gtfs_df/utils.rb` but it's not used anywhere.
    I haven't figured out how to properly implement with Polars.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/davidmh/ruby-gtfs_df.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

[GTFS]: https://gtfs.org/
[Polars]: https://pola.rs/
[ruby-polars]: https://github.com/ankane/ruby-polars
[partridge]: https://github.com/remix/partridge
