require 'rake/javaextensiontask'
require 'rake/testtask'
require 'rubygems'
require 'bundler/setup'

Rake::JavaExtensionTask.new('spymemcached_adapter') do |ext|
  jruby_jar = File.join(ENV['RBENV_ROOT'], 'versions', ENV['RBENV_VERSION'], 'lib', 'jruby.jar')
  jars = [jruby_jar] + FileList['lib/spymemcached-*.jar']
  ext.classpath = jars.map {|x| File.expand_path x}.join(':')
  ext.target_version = 1.6
  ext.source_version = 1.6
end

Rake::TestTask.new(:unit_test) do |t|
  t.libs << 'test'
  t.test_files = FileList['test/*_test.rb']
  t.warning = true
  t.verbose = false
end

task :default => [:test]

task :test => [:compile, :unit_test]
