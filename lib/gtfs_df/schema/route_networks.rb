# frozen_string_literal: true

module GtfsDf
  module Schema
    class RouteNetworks < BaseGtfsTable
      SCHEMA = {
        "route_id" => Polars::String,
        "network_id" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        route_id
        network_id
      ].freeze
    end
  end
end
