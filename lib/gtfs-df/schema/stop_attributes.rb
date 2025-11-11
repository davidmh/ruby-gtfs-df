# frozen_string_literal: true

module GtfsDf
  module Schema
    class StopAttributes < BaseGtfsTable
      SCHEMA = {
        'stop_id' => Polars::String,
        'wheelchair_boarding' => Polars::Int64,
        'bikes_allowed' => Polars::Int64
      }.freeze

      REQUIRED_FIELDS = %w[
        stop_id
      ].freeze
    end
  end
end
