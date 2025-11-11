# frozen_string_literal: true

module GtfsDf
  class SchemaValidator
    def initialize(df, klass)
      @df = df
      @required_fields = klass::REQUIRED_FIELDS
      @schema = klass.const_defined?(:SCHEMA) ? klass::SCHEMA : {}
      @source_class = klass
      @errors = []
      @validated = false
    end

    def valid?
      validate unless @validated
      @errors.empty?
    end

    def errors
      validate unless @validated
      @errors
    end

    private

    def validate
      @errors.clear
      if @df.is_a?(Polars::LazyFrame)
        # Required fields
        @required_fields.each do |field|
          @errors << "#{field}: missing" unless @df.columns.include?(field)
        end

        # Type validation
        schema = @df.schema
        @schema.each do |field, expected_type|
          next unless schema.key?(field)

          actual_type = schema[field]
          if actual_type != expected_type
            @errors << "#{field}: Type mismatch. Expected #{expected_type}, got #{actual_type}"
          end
        end
      # Enum and null checks are skipped for LazyFrame
      # Reason: Both require accessing actual column data, which would
      # materialize the LazyFrame and defeat lazy evaluation. Only schema-based
      # checks (required fields, types) are safe to run on LazyFrame metadata.

      else
        # Required fields
        @required_fields.each do |field|
          if !@df.include?(field)
            @errors << "#{field}: missing"
          elsif @df[field].null_count > 0
            @errors << "#{field}: null"
          end
        end

        # Enum validation
        @schema.each do |field, expected_type|
          next unless @df.include?(field)

          next unless expected_type.is_a?(Polars::Enum)

          allowed = expected_type.categories.to_a
          invalid = @df[field].drop_nulls.to_a.reject { |v| allowed.include?(v.to_s) }
          next if invalid.empty?

          # Try to get value descriptions from ENUM_VALUE_MAP
          value_descs = nil
          if @source_class && @source_class.const_defined?(:ENUM_VALUE_MAP)
            enum_map = @source_class::ENUM_VALUE_MAP
            enum_key = enum_map[field] if enum_map
            if enum_key && GtfsDf::Schema::EnumValues.const_defined?(enum_key)
              value_descs = GtfsDf::Schema::EnumValues.const_get(enum_key)
            end
          end
          allowed_str = if value_descs
                          value_descs.map { |val, desc| "#{val} (#{desc})" }.join(', ')
                        else
                          allowed.join(', ')
                        end
          @errors << "#{field}: Invalid value(s) #{invalid.uniq.join(', ')}. Allowed: #{allowed_str}"
        end
      end
      @validated = true
    end
  end
end
