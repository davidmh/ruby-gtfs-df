# frozen_string_literal: true

module GtfsDf
  module Schema
    class Stops < BaseGtfsTable
      SCHEMA = {
        "stop_id" => Polars::String,
        "stop_code" => Polars::String,
        "stop_name" => Polars::String,
        "tts_stop_name" => Polars::String,
        "stop_desc" => Polars::String,
        "stop_lat" => Polars::Float64,
        "stop_lon" => Polars::Float64,
        "zone_id" => Polars::String,
        "stop_url" => Polars::String,
        "location_type" => Polars::Enum.new(EnumValues::LOCATION_TYPE.map(&:first)),
        "parent_station" => Polars::String,
        "stop_timezone" => Polars::String,
        "wheelchair_boarding" => Polars::Enum.new(EnumValues::WHEELCHAIR_BOARDING.map(&:first)),
        "level_id" => Polars::String,
        "platform_code" => Polars::String,
        "stop_access" => Polars::Enum.new(EnumValues::STOP_ACCESS.map(&:first))
      }

      REQUIRED_FIELDS = %w[stop_id stop_name stop_lat stop_lon].freeze

      ENUM_VALUE_MAP = {
        "location_type" => :LOCATION_TYPE,
        "wheelchair_boarding" => :WHEELCHAIR_BOARDING,
        "stop_access" => :STOP_ACCESS
      }
    end
  end
end
