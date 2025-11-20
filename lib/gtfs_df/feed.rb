# frozen_string_literal: true

module GtfsDf
  class Feed
    GTFS_FILES = %w[
      agency
      stops
      routes
      trips
      stop_times
      calendar
      calendar_dates
      pathways
      levels
      feed_info
      shapes
      frequencies
      transfers
      fare_attributes
      fare_rules
      attributions
      translations
      stop_areas
      stop_attributes
      rider_categories
      fare_media
      fare_products
      fare_leg_rules
      fare_leg_join_rules
      fare_transfer_rules
      areas
      networks
      route_networks
      location_groups
      location_group_stops
      booking_rules
    ].freeze

    attr_reader(*GTFS_FILES)

    # Initialize with a hash of DataFrames
    REQUIRED_GTFS_FILES = %w[agency stops routes trips stop_times].freeze

    def initialize(data = {})
      missing = REQUIRED_GTFS_FILES.reject { |file| data[file].is_a?(Polars::DataFrame) }
      # At least one of calendar or calendar_dates must be present
      unless data["calendar"].is_a?(Polars::DataFrame) || data["calendar_dates"].is_a?(Polars::DataFrame)
        missing << "calendar.txt or calendar_dates.txt"
      end
      unless missing.empty?
        raise GtfsDf::Error, "Missing required GTFS files: #{missing.map do |f|
          f.end_with?(".txt") ? f : f + ".txt"
        end.join(", ")}"
      end

      GTFS_FILES.each do |file|
        df = data[file]
        schema_class_name = file.split("_").map(&:capitalize).join
        schema_class = begin
          GtfsDf::Schema.const_get(schema_class_name)
        rescue
          nil
        end
        if df.is_a?(Polars::DataFrame) && schema_class && schema_class.const_defined?(:SCHEMA)
          df = schema_class.new(df).df
        end
        instance_variable_set("@#{file}", df.is_a?(Polars::DataFrame) ? df : nil)
      end
    end

    # Load from a directory of GTFS CSV files
    def self.load_from_dir(dir)
      data = {}
      GTFS_FILES.each do |file|
        path = File.join(dir, "#{file}.txt")
        next unless File.exist?(path)

        schema_class_name = file.split("_").map(&:capitalize).join

        data[file] = GtfsDf::Schema.const_get(schema_class_name).new(path)
      end
      new(data)
    end

    # Filter the feed using a view hash
    # Example view: { 'routes' => { 'route_id' => '123' }, 'trips' => { 'service_id' => 'A' } }
    def filter(view)
      filtered = {}
      graph = GtfsDf::Graph.build
      # Step 1: Apply view filters
      GTFS_FILES.each do |file|
        df = send(file)
        next unless df

        filters = view[file]
        if filters && !filters.empty?
          filters.each do |col, val|
            df = if val.is_a?(Array)
              df.filter(Polars.col(col).is_in(val))
            elsif val.respond_to?(:call)
              df.filter(val.call(Polars.col(col)))
            else
              df.filter(Polars.col(col).eq(val))
            end
          end
        end
        filtered[file] = df
      end
      # Step 2: Cascade filters following the directed edges
      # An edge from parent->child means: filter child based on valid parent IDs
      changed = true
      while changed
        changed = false
        GTFS_FILES.each do |parent_file|
          parent_df = filtered[parent_file]
          next unless parent_df

          # For each outgoing edge from parent_file to child_file
          graph.adj[parent_file]&.each do |child_file, attrs|
            child_df = filtered[child_file]
            next unless child_df && child_df.height > 0

            attrs[:dependencies].each do |dep|
              parent_col = dep[parent_file]
              child_col = dep[child_file]

              next unless parent_col && child_col &&
                parent_df.columns.include?(parent_col) && child_df.columns.include?(child_col)

              # Get valid values from parent
              valid_values = parent_df[parent_col].to_a.uniq.compact

              # Filter child to only include rows that reference valid parent values
              before = child_df.height
              child_df = child_df.filter(Polars.col(child_col).is_in(valid_values))

              if child_df.height < before
                filtered[child_file] = child_df
                changed = true
              end
            end
          end
        end
      end

      # Remove files that are empty, but keep required files even if empty
      filtered.delete_if do |file, df|
        is_required_file = REQUIRED_GTFS_FILES.include?(file) ||
          file == "calendar" && !filtered["calendar_dates"] ||
          file == "calendar_dates" && !filtered["calendar"]

        (!df || df.height == 0) && !is_required_file
      end
      self.class.new(filtered)
    end
  end
end
