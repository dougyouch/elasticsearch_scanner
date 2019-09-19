# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name        = 'elasticsearch_scanner'
  s.version     = '0.1.0'
  s.licenses    = ['MIT']
  s.summary     = 'ElasticSearch index scanner'
  s.description = 'Iterates over the entire index'
  s.authors     = ['Doug Youch']
  s.email       = 'dougyouch@gmail.com'
  s.homepage    = 'https://github.com/dougyouch/elasticsearch_scanner'
  s.files       = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
end
