# frozen_string_literal: true

require_relative "lib/gtfs_df/version"

Gem::Specification.new do |spec|
  spec.name = "gtfs_df"
  spec.version = GtfsDf::VERSION
  spec.authors = ["David Mejorado"]
  spec.email = ["david.mejorado@gmail.com"]

  spec.summary = "Manipulate GTFS feeds using DataFrames with Polars"
  spec.description = "A Ruby gem to load, filter, and manipulate GTFS (General Transit Feed Specification) feeds using DataFrames powered by Polars. Supports cascading filters that maintain referential integrity across related tables. NOTE: This gem is not ready for production use."
  spec.homepage = "https://github.com/davidmh/ruby-gtfs-df"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.each_line("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "networkx", "~> 0.4"
  spec.add_dependency "polars-df", "~> 0.22", "< 0.24"
  spec.add_dependency "rubyzip", ">= 3.0", "< 4.0"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
