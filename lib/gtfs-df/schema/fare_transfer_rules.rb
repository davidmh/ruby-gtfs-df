# frozen_string_literal: true

module GtfsDf
  module Schema
    class FareTransferRules < BaseGtfsTable
      SCHEMA = {
        "fare_transfer_rule_id" => Polars::String,
        "from_leg_group_id" => Polars::String,
        "to_leg_group_id" => Polars::String,
        "transfer_count" => Polars::Int64,
        "duration_limit" => Polars::Int64,
        "duration_limit_type" => Polars::String,
        "fare_product_id" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        fare_transfer_rule_id
        from_leg_group_id
        to_leg_group_id
      ].freeze
    end
  end
end
