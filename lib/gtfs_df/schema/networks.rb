# frozen_string_literal: true

module GtfsDf
  module Schema
    class Networks < BaseGtfsTable
      SCHEMA = {
        "network_id" => Polars::String,
        "network_name" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        network_id
        network_name
      ].freeze
    end
  end
end
