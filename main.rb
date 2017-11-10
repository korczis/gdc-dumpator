require 'fileutils'
require 'json'
require 'gooddata'

USERNAME = ENV['GDC_USERNAME'] || 'bear@gooddata.com'
PASSWORD = ENV['GDC_PASSWORD'] || ''
SERVER = ENV['GDC_SERVER'] || 'https://instance-develop-45.dev.intgdc.com'
PID = ENV['GDC_PID'] || 'l739tknzsa2b6mp9m49e1s6m1s8jdmhi'

MD_REGEX = %r{/gdc/md/\w+/obj/\d+}

def fetch(client, link)
  puts "Fetching #{link}"

  res = client.get(link)

  items = res['objects']['items']

  next_link = res['objects']['paging']['next']
  items += fetch(client, next_link + '&include=predecessors') if next_link

  items
end

def main
  data_dir = File.join(File.dirname(__FILE__), 'data', PID)
  FileUtils.mkdir_p(data_dir)

  client = GoodData.connect(USERNAME, PASSWORD, server: SERVER, verify_ssl: false)

  response = client.get("/gdc/md/#{PID}/objects/query")

  links = response['about']['links']

  data = links.map do |link|
    begin
      category = link['category']
      items = fetch(client, link['link'].gsub('limit=5', 'limit=50') + '&include=predecessors&deprecated=1')

      category_dir = File.join(data_dir, category)
      FileUtils.mkdir_p(category_dir)

      items.each do |item|
        identifier = item[category]['meta']['identifier']
        item_path = File.join(category_dir, "#{identifier}.json")

        puts "Writing #{item_path}"
        File.open(item_path, 'w') do |f|
          f.write(JSON.pretty_generate(item))
        end
      end

      if category == 'attribute'
        items.each do |item|
          item['attribute']['content']['displayForms'].each do |df|
            df.delete('links')
            df.delete('meta')
          end
        end
      end

      [category, items]
    rescue => e
      puts e.inspect
      nil
    end
  end

  # Remove nil values
  data.compact!

  objects = data.map(&:last)
  objects.flatten!

  lookup = {}
  objects.each do |obj|
    root = obj.keys.first
    raw = obj[root]
    obj_uri = raw['meta']['uri']
    lookup[obj_uri] = obj
  end

  objs_to_check = objects
  while obj = objs_to_check.shift
    root = obj.keys.first
    raw = obj[root]

    JSON.pretty_generate(raw).scan(MD_REGEX).each do |uri|
      next if lookup[uri]

      puts "Second pass fetch '#{uri}'"
      obj = client.get(uri)

      root = obj.keys.first
      raw = obj[root]
      lookup[uri] = obj
      objs_to_check.push(obj)
    end
  end

  lookup.each do |_uri, obj|
    root = obj.keys.first
    raw = obj[root]

  end

  sorted = []
  processed = Set.new
  uris = lookup.keys
  while uri = uris.shift
    toposort(processed, sorted, uri, lookup)
  end

  sorted.uniq!

  sorted.each do |uri|
    obj = lookup[uri]
    key = obj.keys.first
    raw = obj[key]

    puts "#{key}: #{raw['meta']['identifier']} - #{raw['meta']['uri']}"
  end

  generate_metadata_json(sorted, lookup)
end

def toposort(processed, sorted, uri, lookup)
  obj = lookup[uri]

  return if processed.include?(uri)

  if obj.nil?
    puts "Unable to find object #{uri}"
    return
  end

  root = obj.keys.first
  raw = obj[root]

  predecessors = (raw['links'] && raw['links']['predecessors']) || []
  predecessors.each do |predecessor|
    toposort(processed, sorted, predecessor, lookup)
  end

  processed.add(uri)

  sorted.push(uri)
end

def get_raw_object(object)
  root = object.keys.first
  object[root]
end

def generate_name(object)
  root = object.keys.first
  raw = object[root]
  id = raw['meta']['identifier']
  "#{root}.#{id}"
end

def generate_metadata_json(sorted, lookup)
  path = File.join(File.dirname(__FILE__), 'metadata.json')

  objects = sorted.map do |uri|
    lookup[uri]
  end

  data = {
    objects: objects.map do |object|
      root = object.keys.first
      raw = object[root]
      id = raw['meta']['identifier']
      name = "#{root}.#{id}"
      content = JSON.pretty_generate(object)

      content.scan(MD_REGEX).each do |uri|
        target = lookup[uri]
        if target
          content.gsub!(uri, "{{#{generate_name(target)}}}")
        else
          puts "Unable to replace url '#{uri}', object not found"
          puts JSON.pretty_generate(object)
        end
      end

      {
          name: name,
          content: JSON.parse(content)
      }
    end
  }

  File.open('objects.json', 'w') do |f|
    f.write(JSON.pretty_generate(objects))
  end

  File.open('metadata.json', 'w') do |f|
    f.write(JSON.pretty_generate(data))
  end

end

main if __FILE__ == $PROGRAM_NAME
