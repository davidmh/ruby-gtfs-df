# frozen_string_literal: true

module GtfsDf
  module Schema
    class Levels < BaseGtfsTable
      SCHEMA = {
        'level_id' => Polars::String,
        'level_index' => Polars::Float64,
        'level_name' => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[level_id level_index].freeze
    end
  end
end
