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
          # TODO: use `infer_schema: false` instead of `infer_schema_length` after polars release:
          # https://github.com/ankane/ruby-polars/blob/master/CHANGELOG.md#100-unreleased
          df = Polars.read_csv(input, infer_schema_length: 0, encoding: "utf8-lossy")
            .rename(->(col) { col.strip })

          dtypes = self.class::SCHEMA.slice(*df.columns)
          df
            .with_columns(dtypes.keys.map do |col|
              stripped = Polars.col(col).str.strip
              Polars.when(stripped.str.len_chars.gt(0))
                .then(stripped)
                .otherwise(Polars.lit(nil))
            end)
            .with_columns(dtypes.map do |name, type|
                            Polars.col(name).cast(type)
                          end)
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
