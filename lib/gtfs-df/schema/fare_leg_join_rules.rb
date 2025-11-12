# frozen_string_literal: true

module GtfsDf
  module Schema
    class FareLegJoinRules < BaseGtfsTable
      SCHEMA = {
        "fare_leg_join_rule_id" => Polars::String,
        "from_leg_group_id" => Polars::String,
        "to_leg_group_id" => Polars::String,
        "network_id" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        fare_leg_join_rule_id
        from_leg_group_id
        to_leg_group_id
      ].freeze
    end
  end
end
