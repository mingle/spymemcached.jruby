Gem::Specification.new do |s|
  s.name = 'spymemcached.jruby'
  s.version = '1.0.16'
  s.summary = 'A JRuby extension wrapping the latest spymemcached client (v2.12.1).'
  s.description = <<-EOF
A JRuby extension wrapping the latest spymemcached client (v2.12.1).
EOF
  s.license = 'MIT'
  s.authors = ["Xiao Li","Prateek Baheti","Dharmendra Singh" ]
  s.platform = 'java'
  s.email = ['swing1979@gmail.com']
  s.homepage = 'https://github.com/ThoughtWorksStudios/spymemcached.jruby'

  s.add_development_dependency('rake')
  s.add_development_dependency('test-unit')
  s.add_development_dependency('rake-compiler', '~> 0.9', '>= 0.9.2')

  s.files = ['README.md']
  s.files += Dir['lib/**/*']
  s.files += Dir['test/**/*']
end
