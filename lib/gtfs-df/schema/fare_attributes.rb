# frozen_string_literal: true

module GtfsDf
  module Schema
    class FareAttributes < BaseGtfsTable
      SCHEMA = {
        'fare_id' => Polars::String,
        'price' => Polars::Float64,
        'currency_type' => Polars::String,
        'payment_method' => Polars::Int64,
        'transfers' => Polars::Int64,
        'agency_id' => Polars::String,
        'transfer_duration' => Polars::Int64
      }.freeze

      REQUIRED_FIELDS = %w[
        fare_id
        price
        currency_type
        payment_method
        transfers
      ].freeze
    end
  end
end
