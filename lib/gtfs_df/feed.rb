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

    attr_reader(*GTFS_FILES, :graph)

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

      @graph = GtfsDf::Graph.build

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

    # Filter the feed using a view hash
    # Example view: { 'routes' => { 'route_id' => '123' }, 'trips' => { 'service_id' => 'A' } }
    def filter(view)
      filtered = {}

      GTFS_FILES.each do |file|
        df = send(file)
        next unless df

        filtered[file] = df
      end

      # Trips are the atomic unit of GTFS, we will generate a new view
      # based on the set of trips that would be included for each invidual filter
      # and cascade changes from this view in order to retain referential integrity
      trip_ids = nil

      view.each do |file, filters|
        new_filtered = filter!(file, filters, filtered.dup)
        trip_ids = if trip_ids.nil?
          new_filtered["trips"]["trip_id"]
        else
          trip_ids & new_filtered["trips"]["trip_id"]
        end
      end

      if trip_ids
        filtered = filter!("trips", {"trip_id" => trip_ids.to_a}, filtered)
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

    private

    def filter!(file, filters, filtered)
      unless filters.empty?
        df = filtered[file]

        filters.each do |col, val|
          df = if val.is_a?(Array)
            df.filter(Polars.col(col).is_in(val))
          elsif val.respond_to?(:call)
            df.filter(val.call(Polars.col(col)))
          else
            df.filter(Polars.col(col).eq(val))
          end
        end

        filtered[file] = df

        prune!(file, filtered)
      end

      filtered
    end

    def prune!(root, filtered)
      graph.each_bfs_edge(root) do |parent_node_id, child_node_id|
        parent_node = Graph::NODES[parent_node_id]
        child_node = Graph::NODES[child_node_id]
        parent_df = filtered[parent_node.fetch(:file)]
        next unless parent_df

        child_df = filtered[child_node.fetch(:file)]
        # Certain nodes are pre-filtered because they reference only
        # a piece of the dataframe
        filter_attrs = child_node[:filter_attrs]
        if filter_attrs && child_df.columns.include?(filter_attrs.fetch(:filter_col))
          filter = Polars.col(filter_attrs.fetch(:filter_col)).is_in(filter_attrs.fetch(:filter_vals))
          saved_vals = child_df.filter(filter.is_not)
          child_df = child_df.filter(filter)
        end
        next unless child_df && child_df.height > 0

        attrs = graph.get_edge_data(parent_node_id, child_node_id)

        attrs[:dependencies].each do |dep|
          parent_col = dep[parent_node_id]
          child_col = dep[child_node_id]

          next unless parent_col && child_col &&
            parent_df.columns.include?(parent_col) && child_df.columns.include?(child_col)

          # Get valid values from parent
          valid_values = parent_df[parent_col].to_a.uniq.compact

          # Filter child to only include rows that reference valid parent values
          before = child_df.height
          child_df = child_df.filter(
            Polars.col(child_col).is_in(valid_values)
          )
          changed = child_df.height < before

          # If we removed a part of the child_df earlier, concat it back on
          if saved_vals
            child_df = Polars.concat([child_df, saved_vals], how: "vertical")
          end

          if changed
            filtered[child_node.fetch(:file)] = child_df
          end
        end
      end
    end

    def edge_id(parent, child)
      [parent, child].sort.join("-")
    end
  end
end
