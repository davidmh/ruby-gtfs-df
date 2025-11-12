# frozen_string_literal: true

module GtfsDf
  module Schema
    class Pathways < BaseGtfsTable
      SCHEMA = {
        "pathway_id" => Polars::String,
        "from_stop_id" => Polars::String,
        "to_stop_id" => Polars::String,
        "pathway_mode" => Polars::Enum.new(EnumValues::PATHWAY_MODE.map(&:first)),
        "is_bidirectional" => Polars::Enum.new(EnumValues::IS_BIDIRECTIONAL.map(&:first)),
        "length" => Polars::Float64,
        "traversal_time" => Polars::Int64,
        "stair_count" => Polars::Int64,
        "max_slope" => Polars::Float64,
        "min_width" => Polars::Float64,
        "signposted_as" => Polars::String,
        "reversed_signposted_as" => Polars::String
      }

      REQUIRED_FIELDS = %w[pathway_id from_stop_id to_stop_id pathway_mode is_bidirectional].freeze

      ENUM_VALUE_MAP = {
        "pathway_mode" => :PATHWAY_MODE,
        "is_bidirectional" => :IS_BIDIRECTIONAL
      }
    end
  end
end
