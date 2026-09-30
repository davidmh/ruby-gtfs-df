# frozen_string_literal: true

module GtfsDf
  module Schema
    class Directions < BaseGtfsTable
      SCHEMA = {
        "route_id" => Polars::String,
        "direction_id" => Polars::Enum.new(EnumValues::DIRECTION_ID.map(&:first)),
        "direction" => Polars::String
      }

      REQUIRED_FIELDS = %w[route_id direction_id direction].freeze

      ENUM_VALUE_MAP = {
        "direction_id" => :DIRECTION_ID
      }
    end
  end
end
