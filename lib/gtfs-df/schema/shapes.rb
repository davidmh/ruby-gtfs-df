# frozen_string_literal: true

require_relative "../base_gtfs_table"

module GtfsDf
  module Schema
    class Shapes < BaseGtfsTable
      SCHEMA = {
        "shape_id" => Polars::String,
        "shape_pt_lat" => Polars::Float64,
        "shape_pt_lon" => Polars::Float64,
        "shape_pt_sequence" => Polars::Int64,
        "shape_dist_traveled" => Polars::Float64
      }.freeze

      REQUIRED_FIELDS = %w[
        shape_id
        shape_pt_lat
        shape_pt_lon
        shape_pt_sequence
      ].freeze
    end
  end
end
