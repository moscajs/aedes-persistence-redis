local cursor = 0
  local response = {}
  local matches
  local scan
  repeat
    scan = redis.call('SCAN', cursor, 'MATCH', ARGV[1])
    cursor = tonumber(scan[1])
    matches = scan[2]
    for i, key in ipairs(matches) do
      response[i] = key
    end
  until cursor == 0
  return cjson.encode(response)
