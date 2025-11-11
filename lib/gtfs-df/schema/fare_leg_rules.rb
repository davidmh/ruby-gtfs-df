# frozen_string_literal: true

module GtfsDf
  module Schema
    class FareLegRules < BaseGtfsTable
      SCHEMA = {
        'fare_leg_rule_id' => Polars::String,
        'fare_product_id' => Polars::String,
        'from_area_id' => Polars::String,
        'to_area_id' => Polars::String,
        'leg_group_id' => Polars::String,
        'network_id' => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        fare_leg_rule_id
        fare_product_id
      ].freeze
    end
  end
end
