# frozen_string_literal: true

module GtfsDf
  module Schema
    class Trips < BaseGtfsTable
      SCHEMA = {
        "route_id" => Polars::String,
        "service_id" => Polars::String,
        "trip_id" => Polars::String,
        "trip_headsign" => Polars::String,
        "trip_short_name" => Polars::String,
        "direction_id" => Polars::Enum.new(EnumValues::DIRECTION_ID.map(&:first)),
        "block_id" => Polars::String,
        "shape_id" => Polars::String,
        "wheelchair_accessible" => Polars::Enum.new(EnumValues::WHEELCHAIR_ACCESSIBLE.map(&:first)),
        "bikes_allowed" => Polars::Enum.new(EnumValues::BIKES_ALLOWED.map(&:first)),
        "cars_allowed" => Polars::Enum.new(EnumValues::CARS_ALLOWED.map(&:first))
      }

      REQUIRED_FIELDS = %w[route_id service_id trip_id]

      ENUM_VALUE_MAP = {
        "direction_id" => :DIRECTION_ID,
        "wheelchair_accessible" => :WHEELCHAIR_ACCESSIBLE,
        "bikes_allowed" => :BIKES_ALLOWED,
        "cars_allowed" => :CARS_ALLOWED
      }
    end
  end
end
