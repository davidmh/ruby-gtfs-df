# frozen_string_literal: true

module GtfsDf
  class BaseGtfsTable
    attr_reader :df, :validator

    # @param input [Polars::DataFrame, String, Array] A dataframe, a csv path or an array-based table
    def initialize(input)
      @df =
        if input.is_a?(Polars::DataFrame)
          input
        elsif input.is_a?(String)
          # We need to account for extra columns due to: https://github.com/ankane/ruby-polars/issues/125
          all_columns = Polars.scan_csv(input).columns
          default_schema = all_columns.map { |c| [c, Polars::String] }.to_h
          dtypes = default_schema.merge(self.class::SCHEMA)
          Polars.read_csv(input, dtypes:)
        elsif input.is_a?(Array)
          head, *body = input
          df_input = body.each_with_object({}) do |row, acc|
            head.each_with_index do |column, index|
              acc[column] ||= []
              acc[column] << row[index]
            end
          end
          Polars::DataFrame.new(df_input, schema_overrides: self.class::SCHEMA, strict: false)
        else
          throw GtfsDf::Error, "Unrecognized input"
        end
      @validator = SchemaValidator.new(@df, self.class)
    end

    def fields
      self.class::SCHEMA.keys
    end

    def valid?
      @validator.valid?
    end

    def errors
      @validator.errors
    end

    def dataframe
      @df
    end
  end
end
