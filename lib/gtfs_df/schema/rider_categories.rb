# frozen_string_literal: true

module GtfsDf
  module Schema
    class RiderCategories < BaseGtfsTable
      SCHEMA = {
        "rider_category_id" => Polars::String,
        "rider_category_name" => Polars::String,
        "rider_category_description" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        rider_category_id
        rider_category_name
      ].freeze
    end
  end
end
