# frozen_string_literal: true

module GtfsDf
  module Schema
    class FareRules < BaseGtfsTable
      SCHEMA = {
        "fare_id" => Polars::String,
        "route_id" => Polars::String,
        "origin_id" => Polars::String,
        "destination_id" => Polars::String,
        "contains_id" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        fare_id
      ].freeze
    end
  end
end
