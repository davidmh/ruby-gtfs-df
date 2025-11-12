# frozen_string_literal: true

module GtfsDf
  module Schema
    class LocationGroups < BaseGtfsTable
      SCHEMA = {
        "location_group_id" => Polars::String,
        "location_group_name" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        location_group_id
        location_group_name
      ].freeze
    end
  end
end
