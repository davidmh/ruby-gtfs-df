# frozen_string_literal: true

module GtfsDf
  module Schema
    class FareProducts < BaseGtfsTable
      SCHEMA = {
        'fare_product_id' => Polars::String,
        'fare_product_name' => Polars::String,
        'amount' => Polars::Float64,
        'currency' => Polars::String,
        'duration' => Polars::Int64,
        'duration_type' => Polars::String,
        'fare_media_id' => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        fare_product_id
        fare_product_name
        amount
        currency
      ].freeze
    end
  end
end
