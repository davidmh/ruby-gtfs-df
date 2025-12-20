# frozen_string_literal: true

module GtfsDf
  module Schema
    class StopTimes < BaseGtfsTable
      SCHEMA = {
        "trip_id" => Polars::String,
        "arrival_time" => Polars::String,
        "departure_time" => Polars::String,
        "stop_id" => Polars::String,
        "location_group_id" => Polars::String,
        "location_id" => Polars::String,
        "stop_sequence" => Polars::Int64,
        "stop_headsign" => Polars::String,
        "start_pickup_drop_off_window" => Polars::String,
        "end_pickup_drop_off_window" => Polars::String,
        "pickup_type" => Polars::Enum.new(EnumValues::PICKUP_TYPE.map(&:first)),
        "drop_off_type" => Polars::Enum.new(EnumValues::DROP_OFF_TYPE.map(&:first)),
        "continuous_pickup" => Polars::Enum.new(EnumValues::CONTINUOUS_PICKUP.map(&:first)),
        "continuous_drop_off" => Polars::Enum.new(EnumValues::CONTINUOUS_DROP_OFF.map(&:first)),
        "shape_dist_traveled" => Polars::Float64,
        "timepoint" => Polars::Enum.new(EnumValues::TIMEPOINT.map(&:first)),
        "pickup_booking_rule_id" => Polars::String,
        "drop_off_booking_rule_id" => Polars::String
      }

      REQUIRED_FIELDS = %w[trip_id stop_sequence stop_id]

      TIME_FIELDS = %w[
        arrival_time
        departure_time
        start_pickup_drop_off_window
        end_pickup_drop_off_window
      ].freeze

      ENUM_VALUE_MAP = {
        "pickup_type" => :PICKUP_TYPE,
        "drop_off_type" => :DROP_OFF_TYPE,
        "continuous_pickup" => :CONTINUOUS_PICKUP,
        "continuous_drop_off" => :CONTINUOUS_DROP_OFF,
        "timepoint" => :TIMEPOINT
      }
    end
  end
end
