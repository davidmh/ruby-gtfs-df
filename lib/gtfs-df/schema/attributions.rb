# frozen_string_literal: true

module GtfsDf
  module Schema
    class Attributions < BaseGtfsTable
      SCHEMA = {
        'attribution_id' => Polars::String,
        'agency_id' => Polars::String,
        'route_id' => Polars::String,
        'trip_id' => Polars::String,
        'organization_name' => Polars::String,
        'is_producer' => Polars::Int64,
        'is_operator' => Polars::Int64,
        'is_authority' => Polars::Int64,
        'attribution_url' => Polars::String,
        'attribution_email' => Polars::String,
        'attribution_phone' => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        organization_name
        is_producer
        is_operator
        is_authority
      ].freeze
    end
  end
end
