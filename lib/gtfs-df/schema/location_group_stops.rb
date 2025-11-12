# frozen_string_literal: true

module GtfsDf
  module Schema
    class LocationGroupStops < BaseGtfsTable
      SCHEMA = {
        "location_group_id" => Polars::String,
        "stop_id" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        location_group_id
        stop_id
      ].freeze
    end
  end
end
