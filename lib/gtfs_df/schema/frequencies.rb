# frozen_string_literal: true

module GtfsDf
  module Schema
    class Frequencies < BaseGtfsTable
      SCHEMA = {
        "trip_id" => Polars::String,
        "start_time" => Polars::String,
        "end_time" => Polars::String,
        "headway_secs" => Polars::Int64,
        "exact_times" => Polars::Int64
      }.freeze

      REQUIRED_FIELDS = %w[
        trip_id
        start_time
        end_time
        headway_secs
      ].freeze

      TIME_FIELDS = %w[
        start_time
        end_time
      ].freeze
    end
  end
end
