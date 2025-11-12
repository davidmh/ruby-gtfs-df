# frozen_string_literal: true

module GtfsDf
  module Schema
    class Translations < BaseGtfsTable
      SCHEMA = {
        "table_name" => Polars::String,
        "field_name" => Polars::String,
        "language" => Polars::String,
        "translation" => Polars::String,
        "record_id" => Polars::String,
        "record_sub_id" => Polars::String,
        "field_value" => Polars::String
      }.freeze

      REQUIRED_FIELDS = %w[
        table_name
        field_name
        language
        translation
      ].freeze
    end
  end
end
