# frozen_string_literal: true

module GtfsDf
  module Schema
    class Areas < BaseGtfsTable
      SCHEMA = {
        "area_id" => Polars::String,
        "area_name" => Polars::String,
        "area_type" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        area_id
        area_name
      ].freeze
    end
  end
end
