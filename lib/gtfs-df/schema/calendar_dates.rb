# frozen_string_literal: true

module GtfsDf
  module Schema
    class CalendarDates < BaseGtfsTable
      SCHEMA = {
        'service_id' => Polars::String,
        'date' => Polars::String,
        'exception_type' => Polars::Enum.new(EnumValues::EXCEPTION_TYPE.map(&:first))
      }.freeze

      REQUIRED_FIELDS = %w[service_id date exception_type].freeze

      ENUM_VALUE_MAP = {
        'exception_type' => :EXCEPTION_TYPE
      }
    end
  end
end
