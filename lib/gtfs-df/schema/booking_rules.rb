# frozen_string_literal: true

module GtfsDf
  module Schema
    class BookingRules < BaseGtfsTable
      SCHEMA = {
        'booking_rule_id' => Polars::String,
        'booking_type' => Polars::String,
        'min_advance_book_time' => Polars::Int64,
        'max_advance_book_time' => Polars::Int64
      }.freeze

      REQUIRED_FIELDS = %w[
        booking_rule_id
        booking_type
      ].freeze
    end
  end
end
