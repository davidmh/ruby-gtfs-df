# frozen_string_literal: true

module GtfsDf
  module Schema
    class Transfers < BaseGtfsTable
      SCHEMA = {
        'from_stop_id' => Polars::String,
        'to_stop_id' => Polars::String,
        'transfer_type' => Polars::Int64,
        'min_transfer_time' => Polars::Int64
      }.freeze

      REQUIRED_FIELDS = %w[
        from_stop_id
        to_stop_id
        transfer_type
      ].freeze
    end
  end
end
