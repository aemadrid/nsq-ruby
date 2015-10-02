# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |s|
  s.name = 'nsq-ruby'
  s.version = '1.2.1'

  s.required_rubygems_version = Gem::Requirement.new('>= 0') if s.respond_to? :required_rubygems_version=
  s.require_paths = ['lib']
  s.authors = ['Wistia']
  s.date = '2015-04-09'
  s.description = ''
  s.email = 'dev@wistia.com'
  s.extra_rdoc_files = %w( LICENSE.txt README.md )
  s.files = `git ls-files`.split($/)
  s.homepage = 'http://github.com/aemadrid/nsq-ruby'
  s.licenses = ['MIT']
  s.rubygems_version = '2.4.5'
  s.summary = 'Ruby client library for NSQ'

  s.add_dependency 'concurrent-ruby', '1.0.0.pre3'
  s.add_dependency 'concurrent-ruby-edge', '0.2.0.pre3'

  s.add_runtime_dependency 'http'
  s.add_runtime_dependency 'multi_json'
  s.add_runtime_dependency 'digest-crc'

  s.add_runtime_dependency 'pry'

  s.add_development_dependency 'rake'
  s.add_development_dependency 'childprocess'
  s.add_development_dependency 'rspec', '~> 3.0'

end

