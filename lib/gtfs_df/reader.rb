# frozen_string_literal: true

module GtfsDf
  class Reader
    # Loads a GTFS zip file and returns a Feed
    def self.load_from_zip(zip_path)
      data = nil

      Dir.mktmpdir do |tmpdir|
        Zip::File.open(zip_path) do |zip_file|
          zip_file.each do |entry|
            next unless entry.file?
            out_path = File.join(tmpdir, entry.name)
            entry.extract(out_path)
          end
        end

        data = load_from_dir(tmpdir)
      end

      data
    end

    # Loads a GTFS dir and returns a Feed
    def self.load_from_dir(dir_path)
      data = {}
      GtfsDf::Feed::GTFS_FILES.each do |gtfs_file|
        path = File.join(dir_path, "#{gtfs_file}.txt")
        next unless File.exist?(path)

        data[gtfs_file] = data_frame(gtfs_file, path)

        # Explicit handling for parent stations. We separate these out to avoid
        # a self-loop and recombine when writing. If multiple files end up having
        # self-loops, we would be better off generalizing the graph logic.
        if gtfs_file == "stops" && data["stops"].columns.include?("location_type")
          df = data["stops"]
          data["stops"] = df.filter(
            Polars.col("location_type").is_in(
              GtfsDf::Schema::EnumValues::STOP_LOCATION_TYPES.map(&:first)
            )
          )
          data["parent_stations"] = df.filter(
            Polars.col("location_type").is_in(
              GtfsDf::Schema::EnumValues::STATION_LOCATION_TYPES.map(&:first)
            )
          )
        end
      end

      GtfsDf::Feed.new(data)
    end

    private_class_method def self.data_frame(gtfs_file, path)
      schema_class_name = gtfs_file.split("_").map(&:capitalize).join
      GtfsDf::Schema.const_get(schema_class_name).new(path).df
    end
  end
end
