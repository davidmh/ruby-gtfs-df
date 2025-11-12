# frozen_string_literal: true

module GtfsDf
  module Schema
    class FareMedia < BaseGtfsTable
      SCHEMA = {
        "fare_media_id" => Polars::String,
        "fare_media_name" => Polars::String,
        "fare_media_type" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        fare_media_id
        fare_media_name
      ].freeze
    end
  end
end
