# frozen_string_literal: true

module GtfsDf
  class Reader
    # Loads a GTFS zip file and returns a Feed
    def self.load_from_zip(zip_path)
      data = {}
      Dir.mktmpdir do |tmpdir|
        Zip::File.open(zip_path) do |zip_file|
          zip_file.each do |entry|
            next unless entry.file?

            GtfsDf::Feed::GTFS_FILES.each do |gtfs_file|
              next unless entry.name == "#{gtfs_file}.txt"

              out_path = File.join(tmpdir, entry.name)
              entry.extract(out_path)

              data[gtfs_file] = data_frame(gtfs_file, out_path)
            end
          end
        end
      end
      GtfsDf::Feed.new(data)
    end

    # Loads a GTFS dir and returns a Feed
    def self.load_from_dir(dir_path)
      data = {}
      GtfsDf::Feed::GTFS_FILES.each do |gtfs_file|
        path = File.join(dir_path, "#{gtfs_file}.txt")
        next unless File.exist?(path)

        data[gtfs_file] = data_frame(gtfs_file, path)
      end

      GtfsDf::Feed.new(data)
    end

    private_class_method def self.data_frame(gtfs_file, path)
      schema_class_name = gtfs_file.split("_").map(&:capitalize).join
      GtfsDf::Schema.const_get(schema_class_name).new(path).df
    end
  end
end
