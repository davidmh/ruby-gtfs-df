# frozen_string_literal: true

require "gtfs_df"
require "pry-byebug"

# Custom schema class for testing extra parameter across reader, feed, graph, etc.
class Directions < GtfsDf::BaseGtfsTable
  SCHEMA = {
    "route_id" => Polars::String,
    "direction_id" => Polars::Enum.new(GtfsDf::Schema::EnumValues::DIRECTION_ID.map(&:first)),
    "direction" => Polars::String
  }

  REQUIRED_FIELDS = %w[route_id direction_id direction].freeze

  ENUM_VALUE_MAP = {
    "direction_id" => :DIRECTION_ID
  }
end

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  # Inject util helpers to have them readily available in the test env
  config.include GtfsDf::Utils
end
