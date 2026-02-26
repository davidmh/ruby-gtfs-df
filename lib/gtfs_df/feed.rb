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

    attr_accessor(*GTFS_FILES)
    attr_accessor(:parse_times)
    attr_reader(:graph)

    # Initialize with a hash of DataFrames
    REQUIRED_GTFS_FILES = %w[agency stops routes trips stop_times].freeze

    # @param data [Hash] Hash of DataFrames for each GTFS file
    # @param parse_times [Boolean] Whether to parse time fields to seconds since midnight (default: false)
    def initialize(data = {}, parse_times: false)
      @parse_times = parse_times

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
          # Parse time fields if enabled and they're still strings
          if @parse_times && schema_class.respond_to?(:time_fields)
            time_fields = schema_class.time_fields
            time_fields.each do |field|
              next unless df.columns.include?(field)
              # Only parse if the field is still a string (not already parsed)
              if df[field].dtype == Polars::String
                df = df.with_columns(
                  GtfsDf::Utils.as_seconds_since_midnight(field)
                )
              end
            end
          end
        end
        instance_variable_set("@#{file}", df.is_a?(Polars::DataFrame) ? df : nil)
      end
    end

    # Filter the feed using a view hash
    #
    # @param view [Hash] The view used to filter the feed, with format { file => filters }.
    #   Example view: { 'routes' => { 'route_id' => '123' }, 'trips' => { 'service_id' => 'A' } }
    # @param filter_only_children [Boolean] Whether only child dependencies should be pruned.
    #   When false, we:
    #   - Treat trips as the atomic unit of GTFS. Therefore, if we filter to one stop
    #     referenced by TripA, we will preserve _all stops_ referenced by TripA.
    #   - Prune unreferenced parent objects (e.g. route is a parent of trip. Unreferenced routes
    #     will be pruned.)
    #   When true we:
    #   - Do not treat trips as atomic. I can filter stopA without maintaining other stops for
    #     trips that reference it.
    #   - Only filter child objects
    def filter(view, filter_only_children: false)
      filtered = {}

      GTFS_FILES.each do |file|
        df = send(file)
        next unless df

        filtered[file] = df
      end

      if filter_only_children
        view.each do |file, filters|
          filtered = filter!(file, filters, filtered.dup, filter_only_children: true)
        end
      else
        # Trips are the atomic unit of GTFS, we will generate a new view
        # based on the set of trips that would be included for each invidual filter
        # and cascade changes from this view in order to retain referential integrity
        trip_ids = Polars::Series.new.alias("trip_id")

        view.each do |file, filters|
          new_filtered = filter!(file, filters, filtered.dup)
          trip_ids = if trip_ids.empty?
            new_filtered["trips"]["trip_id"]
          else
            trip_ids.filter(trip_ids.is_in(new_filtered["trips"]["trip_id"]))
          end
        end

        if trip_ids
          filtered = filter!("trips", {"trip_id" => trip_ids.implode}, filtered.dup)
        end
      end

      # Remove files that are empty, but keep required files even if empty
      filtered.delete_if do |file, df|
        is_required_file = REQUIRED_GTFS_FILES.include?(file) ||
          file == "calendar" && !filtered["calendar_dates"] ||
          file == "calendar_dates" && !filtered["calendar"]

        (!df || df.height == 0) && !is_required_file
      end
      self.class.new(filtered, parse_times: @parse_times)
    end

    # Utility method that returns a hash of dataframes by file name
    #
    # @return [{file_name => dataframe}]
    def by_dataframe_name
      GTFS_FILES.filter_map do |file|
        dataframe = send(file)
        dataframe ? [file, dataframe] : nil
      end.to_h
    end

    # Utility method for getting a dataframe, e.g. feed['agency']
    #
    # @param [string] file name
    # @return [dataframe]
    def [](file_name)
      send(file_name)
    end

    # Utility method for setting a dataframe, e.g. feed['agency'] = new_dataframe
    #
    # @param [string] file name
    # @value [dataframe] the new dataframe
    def []=(file_name, value)
      send("#{file_name}=", value)
    end

    private

    def filter!(file, filters, filtered, filter_only_children: false)
      unless filters.empty?
        df = filtered[file]

        filters.each do |col, val|
          df = if val.is_a?(Polars::Series) || val.is_a?(Array)
            df.filter(Polars.col(col).is_in(val))
          elsif val.respond_to?(:call)
            df.filter(val.call(Polars.col(col)))
          else
            df.filter(Polars.col(col).eq(val))
          end
        end

        filtered[file] = df

        prune!(file, filtered, filter_only_children:)
      end

      filtered
    end

    # Traverses the graph to prune unreferenced entities from child dataframes
    # based on parent relationships. See GtfsDf::Graph::STOP_NODES
    #
    # The trips table has multiple parents (calendar, calendar_dates, routes,
    # stop_times). We accumulate valid values from all of them and keep rows
    # that match any parent, so trips referenced only via calendar_dates are
    # not dropped when another edge is processed first.
    def prune!(root, filtered, filter_only_children: false)
      seen_edges = Set.new
      rerooted_graph = Graph.build(bidirectional: !filter_only_children)
      trips_accumulated = {} # [child_col] => Polars::Series (unique values)
      trips_allow_null = {} # [child_col] => true/false
      trips_base_df = nil

      queue = [root]

      while queue.length > 0
        parent_node_id = queue.shift
        rerooted_graph.adj[parent_node_id].each do |child_node_id, attrs|
          edge = edge_id(parent_node_id, child_node_id)

          next if seen_edges.include?(edge)
          seen_edges.add(edge)

          parent_node = Graph::NODES[parent_node_id]
          child_node = Graph::NODES[child_node_id]
          parent_df = filtered[parent_node.fetch(:file)]
          next unless parent_df

          child_df = filtered[child_node.fetch(:file)]
          # Certain nodes are pre-filtered because they reference only
          # a piece of the dataframe
          filter_attrs = child_node[:filter_attrs]
          if filter_attrs && child_df.columns.include?(filter_attrs.fetch(:filter_col))
            filter = filter_attrs.fetch(:filter)
            # Temporarily remove rows that do not match node filter criteria to process them
            # separately (e.g., when filtering stops, parent stations that should be preserved
            # regardless of direct references)
            saved_vals = child_df.filter(filter.is_not)
            child_df = child_df.filter(filter)
          end
          next unless child_df && child_df.height > 0

          queue << child_node_id

          # If the edge is weak (e.g. reverse edge of an optional relationship),
          # we traverse to ensure connectivity but do NOT apply the filter.
          if attrs[:type] == :weak
            # puts "Skipping weak filter: #{edge}"
            next
          end

          attrs[:dependencies].each do |dep|
            parent_col = dep[parent_node_id]
            child_col = dep[child_node_id]
            allow_null_flag = !!dep[:allow_null]

            next unless parent_col && child_col &&
              parent_df.columns.include?(parent_col) && child_df.columns.include?(child_col)

            # Get valid values from parent
            valid_values = parent_df[parent_col].drop_nulls.unique

            if child_node_id == "trips" && (parent_node_id == "calendar" || parent_node_id == "calendar_dates")
              # Calendar + calendar_dates both define service for the same trips, so we want
              # union semantics across those two parents (a trip is valid if it appears in
              # either).
              #
              # Here we accumulate valid service_ids across calendar/calendar_dates, but only
              # within the pool of trips that are already reachable from structural parents.
              key = child_col
              trips_accumulated[key] = if trips_accumulated[key]
                Polars.concat([trips_accumulated[key], valid_values]).unique
              else
                valid_values
              end
              trips_allow_null[key] = allow_null_flag

              # Determine the base pool of trips:
              # - If we've already restricted trips via structural parents (routes,
              #   stop_times, shapes, etc), use that as the base.
              # - Otherwise, like when filtering directly on trips, use the current
              #   trips dataframe.
              trips_base_df ||= filtered[child_node.fetch(:file)]
              next unless trips_base_df && trips_base_df.height > 0

              conditions = trips_accumulated.map do |col, vals|
                c = Polars.col(col).is_in(vals.implode)
                c = (c | Polars.col(col).is_null) if trips_allow_null[col]
                c
              end
              combined = conditions.reduce { |a, b| a | b }
              filtered[child_node.fetch(:file)] = trips_base_df.filter(combined)
            else
              # Original single-edge logic for all other nodes
              before = child_df.height

              cond = Polars.col(child_col).is_in(valid_values.implode)
              cond = (cond | Polars.col(child_col).is_null) if allow_null_flag
              child_df = child_df.filter(cond)

              if child_df.height < before
                child_df = Polars.concat([child_df, saved_vals], how: "vertical") if saved_vals
                filtered[child_node.fetch(:file)] = child_df
              end
            end
          end
        end
      end
    end

    def edge_id(parent, child)
      [parent, child].join("-")
    end
  end
end
