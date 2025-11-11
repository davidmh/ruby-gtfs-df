# frozen_string_literal: true

module GtfsDf
  module Schema
    class Agency < BaseGtfsTable
      SCHEMA = {
        'agency_id' => Polars::String,
        'agency_name' => Polars::String,
        'agency_url' => Polars::String,
        'agency_timezone' => Polars::String,
        'agency_lang' => Polars::String,
        'agency_phone' => Polars::String,
        'agency_fare_url' => Polars::String,
        'agency_email' => Polars::String,
        'ticketing_deep_link_id' => Polars::String, # Google extension, optional
        'cemv_support' => Polars::Enum.new(EnumValues::CEMV_SUPPORT.map(&:first)) # GTFS extension, optional
      }

      REQUIRED_FIELDS = %w[agency_name agency_url agency_timezone].freeze

      ENUM_VALUE_MAP = {
        'cemv_support' => :CEMV_SUPPORT
      }
    end
  end
end
