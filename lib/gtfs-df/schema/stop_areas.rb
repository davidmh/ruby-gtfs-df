# frozen_string_literal: true

module GtfsDf
  module Schema
    class StopAreas < BaseGtfsTable
      SCHEMA = {
        "stop_area_id" => Polars::String,
        "stop_area_name" => Polars::String,
        "stop_area_type" => Polars::String,
        "parent_stop_area_id" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        stop_area_id
        stop_area_name
      ].freeze
    end
  end
end
