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
          filtered = filter!("trips", {"trip_id" => trip_ids.to_a}, filtered.dup)
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

    private

    def filter!(file, filters, filtered, filter_only_children: false)
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

        prune!(file, filtered, filter_only_children:)
      end

      filtered
    end

    # Traverses the graph to prune unreferenced entities from child dataframes
    # based on parent relationships. See GtfsDf::Graph::STOP_NODES
    def prune!(root, filtered, filter_only_children: false)
      seen_edges = Set.new

      queue = [root]

      while queue.length > 0
        parent_node_id = queue.shift
        # Prune edges from the node to its children
        graph.adj[parent_node_id].each do |child_node_id, attrs|
          prune_edge(parent_node_id, child_node_id, attrs, queue, seen_edges, filtered)
        end

        # If not filtering only children, we also need to prune edges from the node
        # to its predecessors
        unless filter_only_children
          graph.pred[parent_node_id].each do |pred_node_id, attrs|
            prune_edge(parent_node_id, pred_node_id, attrs, queue, seen_edges, filtered)
          end
        end
      end
    end

    def prune_edge(u_id, v_id, attrs, queue, seen_edges, filtered)
      edge = edge_id(u_id, v_id)

      return if seen_edges.include?(edge)
      seen_edges.add(edge)

      node_u = Graph::NODES[u_id]
      node_v = Graph::NODES[v_id]
      u_df = filtered[node_u.fetch(:file)]
      return unless node_u

      v_df = filtered[node_v.fetch(:file)]
      # Certain nodes are pre-filtered because they reference only
      # a piece of the dataframe
      filter_attrs = node_v[:filter_attrs]
      if filter_attrs && v_df.columns.include?(filter_attrs.fetch(:filter_col))
        filter = filter_attrs.fetch(:filter)
        # Temporarily remove rows that do not match node filter criteria to process them
        # separately (e.g., when filtering stops, parent stations that should be preserved
        # regardless of direct references)
        saved_vals = v_df.filter(filter.is_not)
        v_df = v_df.filter(filter)
      end
      return unless v_df && v_df.height > 0

      queue << v_id

      attrs[:dependencies].each do |dep|
        u_col = dep[u_id]
        v_col = dep[v_id]
        allow_null = !!dep[:allow_null]

        # TODO: I wonder if this should just be another annoying
        # special case instead of having to add a new dependency property
        next if u_id == "frequencies" && v_id == "trips"

        next unless u_col && v_col &&
          u_df.columns.include?(u_col) && v_df.columns.include?(v_col)

        # Get valid values from parent
        valid_values = u_df[u_col].to_a.uniq.compact

        # Annoying special case to make sure that if we have a calendar with exceptions,
        # the calendar_dates file doesn't end up pruning other files
        if u_id == "calendar_dates" && u_col == "service_id" &&
            filtered["calendar"]
          valid_values = (valid_values + calendar["service_id"].to_a).uniq
        end

        # Filter child to only include rows that reference valid parent values
        before = v_df.height
        filter = Polars.col(v_col).is_in(valid_values)
        if allow_null
          filter = (filter | Polars.col(v_col).is_null)
        end
        v_df = v_df.filter(filter)
        changed = v_df.height < before

        # If we removed a part of the child_df earlier, concat it back on
        if saved_vals
          v_df = Polars.concat([v_df, saved_vals], how: "vertical")
        end

        if changed
          filtered[node_v.fetch(:file)] = v_df
        end
      end
    end

    def edge_id(parent, child)
      [parent, child].join("-")
    end
  end
end
