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

            GtfsDf::Feed::GTFS_FILES.each do |file|
              next unless entry.name == "#{file}.txt"

              out_path = File.join(tmpdir, entry.name)
              entry.extract(out_path)
              schema_class_name = file.split("_").map(&:capitalize).join

              data[file] = GtfsDf::Schema.const_get(schema_class_name).new(out_path).df
            end
          end
        end
      end
      GtfsDf::Feed.new(data)
    end
  end
end
