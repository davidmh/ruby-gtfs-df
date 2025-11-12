# frozen_string_literal: true

module GtfsDf
  module Schema
    class Routes < BaseGtfsTable
      SCHEMA = {
        "route_id" => Polars::String,
        "agency_id" => Polars::String,
        "route_short_name" => Polars::String,
        "route_long_name" => Polars::String,
        "route_desc" => Polars::String,
        "route_type" => Polars::String,
        "route_url" => Polars::String,
        "route_color" => Polars::String,
        "route_text_color" => Polars::String,
        "route_sort_order" => Polars::String,
        "continuous_pickup" => Polars::Int64,
        "continuous_drop_off" => Polars::Int64,
        "network_id" => Polars::String,
        "cemv_support" => Polars::Enum.new(EnumValues::CEMV_SUPPORT.map(&:first))
      }

      REQUIRED_FIELDS = %w[route_id route_type].freeze

      ENUM_VALUE_MAP = {
        "route_type" => :ROUTE_TYPE,
        "continuous_pickup" => :CONTINUOUS_PICKUP,
        "continuous_drop_off" => :CONTINUOUS_DROP_OFF,
        "cemv_support" => :CEMV_SUPPORT
      }
    end
  end
end
