# frozen_string_literal: true

module GtfsDf
  module Schema
    class Calendar < BaseGtfsTable
      SCHEMA = {
        "service_id" => Polars::String,
        "monday" => Polars::Enum.new(EnumValues::SERVICE_DAY.map(&:first)),
        "tuesday" => Polars::Enum.new(%w[0 1]),
        "wednesday" => Polars::Enum.new(%w[0 1]),
        "thursday" => Polars::Enum.new(EnumValues::SERVICE_DAY.map(&:first)),
        "friday" => Polars::Enum.new(%w[0 1]),
        "saturday" => Polars::Enum.new(%w[0 1]),
        "sunday" => Polars::Enum.new(%w[0 1]),
        "start_date" => Polars::String,
        "end_date" => Polars::String
      }

      REQUIRED_FIELDS = %w[service_id monday tuesday wednesday thursday friday saturday sunday start_date end_date]

      ENUM_VALUE_MAP = {
        "monday" => :SERVICE_DAY,
        "tuesday" => :SERVICE_DAY,
        "wednesday" => :SERVICE_DAY,
        "thursday" => :SERVICE_DAY,
        "friday" => :SERVICE_DAY,
        "saturday" => :SERVICE_DAY,
        "sunday" => :SERVICE_DAY
      }
    end
  end
end
