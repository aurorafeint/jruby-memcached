# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "memcached/version"

Gem::Specification.new do |s|
  s.name        = "jruby-memcached"
  s.version     = Memcached::VERSION
  s.authors     = ["Richard Huang"]
  s.email       = ["flyerhzm@gmail.com"]
  s.homepage    = ""
  s.summary     = %q{jruby compatible memcached client}
  s.description = %q{jruby memcacached client which is compatible with memcached gem}

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'mocha'
end
