# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'file_loader/version'

Gem::Specification.new do |spec|
  spec.name          = "file_loader"
  spec.version       = FileLoader::VERSION
  spec.authors       = ["novikov"]
  spec.email         = ["novikov@inventos.ru"]
  spec.summary       = %q{File loader}
  spec.description   = %q{File loader}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "pry"
  spec.add_development_dependency "pry-byebug"
end
